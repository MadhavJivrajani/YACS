import sys
import json
import threading
import socket
import signal
import logging

from typing import List
from queue import Queue

from yacs.component.listener import Listener
from yacs.component.scheduler import Scheduler
from yacs.utils.errors import *

__all__ = ['Master']

class Master:
	"""Logic that implements a master node in the cluster. 

	It handles the following:

	- Listen for job requests (on port ``5000``)
	- Listen for updates from workers (on port ``5001``)
	- Perform scheduling of tasks based on the specifed policy
	"""
	def __init__(self) -> None:
		self.__sched_policies = ["LL", "RR", "R"]
		self.sched_policy = "LL"

		logging.info("scheduling policy set to %s" % self.sched_policy)
		self.worker_config = dict()

		# in case functionality for providing updates to clients is provided.
		self.__job_listeners = {}
		self.__update_listeners = {}

		# set default ip to localhost, can be configured
		self.master_ip = "127.0.0.1"

		# register signal handlers
		self.__register_signal_handlers({
			signal.SIGTERM	: handle_thread_terminate,
			signal.SIGINT	: handle_thread_terminate,
		})

		# queues used for inter-thread communication
		self.__job_queue = Queue()
		self.__update_queue = Queue()
		self.scheduled_tasks_queue = Queue()

		# this queue is meant to synchronize calls to
		# trigger scheduler
		self.__sched_queue = Queue()

		# data structures to keep track of tasks/jobs and aid
		# in scheduling decisions.
		self.jobs = {}
		self.tasks_pool = Queue()
		self.slots_free = {}
		self.worker_ports = {}

		self.scheduler = Scheduler(self, self.sched_policy)
		self.scheduler_lock = threading.Lock()

	def get_sched_polices(self) -> List[str]:
		"""Return a list of available scheduling policies
		
		The following are available:

		- LL: Least Loaded
		- RR: Round Robin
		- R : Random
		"""
		return self.__sched_policies

	def config(self, path_to_config: str) -> object:
		"""Read in config file for worker information and
		set data structures realted to workers and initialise
		the number of free ``slots`` per worker

		For format of config file, please refer :ref:`dev`

		:param path_to_config: path to config file on local system
		:type path_to_config: `str`
		"""
		with open(path_to_config, "r") as conf:
			self.worker_config = json.load(conf)

		# set data structures related to workers and 
		# initialise the number of free slots for each 
		# worker.
		for worker_info in self.worker_config["workers"]:
			worker_id = worker_info['worker_id']
			slots = worker_info["slots"]
			port = worker_info["port"]
			self.slots_free[worker_id] = slots
			self.worker_ports[worker_id] = port

		return self

	# poll the job queue to check for new job submissions
	# from client(s)
	def __poll_job_queue(self) -> None:
		while True:
			# get() in Queue is a blocking call
			# will block here if queue is empty
			client, job = self.__job_queue.get()
			logging.info("scheduling job: %s recieved from ip: %s port: %d"
						 % (job["job_id"], client[0], client[1]))

			# 'notify' the queue that the current item
			# is done processing
			self.__job_queue.task_done()

			job_id = job['job_id']
			raw_map_tasks = job['map_tasks']
			raw_reduce_tasks = job['reduce_tasks']

			map_tasks = []
			for map_task in raw_map_tasks:
				map_task['job_id'] = job_id
				map_task['type'] = 'map'
				map_tasks.append(map_task)

			reduce_tasks = []
			for reduce_task in raw_reduce_tasks:
				reduce_task['job_id'] = job_id
				reduce_task['type'] = 'reduce'
				reduce_tasks.append(reduce_task)

			# each job will be on this form
			job_object = {
				'total_map_tasks': len(map_tasks),
				'scheduled_map_tasks': 0,
				'completed_map_tasks': 0,
				'total_reduce_tasks': len(reduce_tasks),
				'scheduled_reduce_tasks': 0,
				'completed_reduce_tasks': 0,
				'reduce_tasks': reduce_tasks
			}
			with self.scheduler_lock:
				self.jobs[job_id] = job_object

				if len(map_tasks) != 0:
					list(map(self.tasks_pool.put, map_tasks)) # enqueue map tasks
				else:
					list(map(self.tasks_pool.put, reduce_tasks)) # enqueue reduce tasks
			self.__sched_queue.put(1)

	# poll the update queue to retrieve any updates
	# sent by the master and update data structures 
	# appropriately
	def __poll_update_queue(self) -> None:
		while True:
			# get() in Queue is a blocking call
			# will block here if queue is empty
			worker, update = self.__update_queue.get()
			logging.info("received update from %s at ip: %s port: %d"
						 % (update["worker_id"], worker[0], worker[1]))

			# 'notify' the queue that the current item
			# is done processing
			self.__update_queue.task_done()

			worker_id = update['worker_id']
			tasks = update['data']

			with self.scheduler_lock:
				self.slots_free[worker_id] += len(tasks)

			completed_jobs = []

			for task in tasks:

				job_id = task['job_id']
				task_type = task['type']

				logging.info("completed task %s" % (task["task_id"]))

				# acquire lock and update data structures
				# while taking care of the map reduce dependency
				# constraint
				with self.scheduler_lock:
					if task_type == 'map':
						self.jobs[job_id]['completed_map_tasks'] += 1
						if self.jobs[job_id]['completed_map_tasks'] == \
							self.jobs[job_id]['total_map_tasks']:
							list(map(self.tasks_pool.put, self.jobs[job_id]['reduce_tasks']))
					else:
						self.jobs[job_id]['completed_reduce_tasks'] += 1
						if self.jobs[job_id]['completed_reduce_tasks'] == \
							self.jobs[job_id]['total_reduce_tasks']:
							completed_jobs.append(job_id)
							logging.info("job %s completed" % (job_id))

			# delete completed jobs from the 
			# current jobs dict
			for job in completed_jobs:
				del self.jobs[job]

			self.__sched_queue.put(1)

	# poll the sched_queue to synchronize
	# calls to the scheduler
	def __poll_scheduler(self):
		while True:
			_ = self.__sched_queue.get()
			self.__sched_queue.task_done()

			self.scheduler.schedule_tasks()

	# send task requests to a specified worker
	# decided by the scheduler
	def __send_tasks_requests(self) -> None:
		while True:
			task = self.scheduled_tasks_queue.get()

			task_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			self.scheduled_tasks_queue.task_done()
			worker_id = task["worker_id"]
			port = self.worker_ports[worker_id]

			task_socket.connect((self.master_ip, port))

			json_data = json.dumps(task)
			task_socket.sendall(json_data.encode("utf-8"))

			task_socket.close()

	def set_sched_policy(self, sched_policy: str = "LL") -> object:
		"""Sets the scheduling policy for the ``Master``

		:param sched_policy: scheduling policy to be configured for the ``Master``
		:type sched_policy: `str`
		"""
		self.sched_policy = sched_policy
		self.scheduler = Scheduler(self, self.sched_policy)

		return self

	def set_master_ip(self, ip: str = "127.0.0.1") -> object:
		"""Set ip address of master
		
		:param ip: ip address of master node
		:type ip: `str`
		"""
		self.master_ip = ip
		return self

	def start(self) -> None:
		"""Responsible for spawning threads and catching
		signals for graceful termination.

		Threads spawned:

		- Listen to incoming job requests
		- Listen to updates from workers
		- Poll job queue
		- Poll update queue
		- Send tasks to workers to be executed

		For more details refer :ref:`dev`
		"""
		try:
			job_listener = self.__spawn(
				self.__spawn_job_listener, args=(5000,))
			logging.info("job listener spawn successful")

			update_listener = self.__spawn(
				self.__spawn_update_listener, args=(5001,))
			logging.info("update listener spawn successful")

			job_queue_poll = self.__spawn(self.__poll_job_queue)
			logging.info("job queue spawn successful")

			update_queue_poll = self.__spawn(self.__poll_update_queue)
			logging.info("update queue spawn successful")

			send_tasks = self.__spawn(self.__send_tasks_requests)
			sched_tasks = self.__spawn(self.__poll_scheduler)

			job_listener.join()
			update_listener.join()
			job_queue_poll.join()
			update_queue_poll.join()
			send_tasks.join()
			sched_tasks.join()

		except ThreadTerminate:
			logging.info("shutting down listeners")

	def __send_msg(self, addr, listeners) -> None:
		listeners[addr].ack_flag.set()

	# utility function to spwan a thread and return thread object
	def __spawn(self, func, args: tuple = ()) -> threading.Thread:
		thread = threading.Thread(target=func, args=args)
		thread.daemon = True
		thread.start()

		return thread

	# handle concurrent client connections
	def __spawn_job_listener(self, port: int = 5000) -> None:
		job_socket = socket.create_server((self.master_ip, port))
		job_socket.listen(1)

		while True:
			(client, client_addr) = job_socket.accept()

			job_listener = Listener(client, client_addr, self.__job_queue, "JOB_LISTENER")
			self.__job_listeners[client_addr] = job_listener
			job_listener.daemon = True
			job_listener.start()

	# handle concurrent update connections
	def __spawn_update_listener(self, port: int = 5001) -> None:
		update_socket = socket.create_server((self.master_ip, port))
		update_socket.listen(1)

		while True:
			(worker, worker_addr) = update_socket.accept()

			update_listener = Listener(worker, worker_addr, self.__update_queue, "UPDATE_LISTENER")
			update_listener.daemon = True
			self.__update_listeners[worker_addr] = update_listener
			update_listener.start()

	# register signal handlers for graceful termination
	def __register_signal_handlers(self, handler_map: dict) -> None:
		for sig, handler in handler_map.items():
			signal.signal(sig, handler)

if __name__ == '__main__':
	try:
		path = sys.argv[1]
		sched_policy = sys.argv[2]
		logging.basicConfig(
			filename="yacs.log",
			format="%(levelname)s %(asctime)s.%(msecs)03d: %(message)s",
			datefmt="%m/%d/%Y %H:%M:%S",
			level=logging.DEBUG,
		)
		m = Master()\
			.config(path_to_config=path)\
			.set_sched_policy(sched_policy=sched_policy)
		m.start()

	except Exception as e:
		logging.error(e)

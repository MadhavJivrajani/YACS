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
from yacs.utils.errors import handle_thread_terminate, ThreadTerminate

__all__ = ['Master']


class Master:
	def __init__(self) -> None:
		self.__sched_policies = ["LL", "RR", "R"]
		self.sched_policy = "LL"

		logging.info("scheduling policy set to %s" % self.sched_policy)
		self.worker_config = dict()

		# in case functionality for providing updates to clients is provided.
		self.__job_listeners = {}
		self.__update_listeners = {}

		self.master_ip = "127.0.0.1"

		# register signal handlers
		self.__register_signal_handlers({
			signal.SIGTERM	: handle_thread_terminate,
			signal.SIGINT	: handle_thread_terminate,
		})

		self.__job_queue = Queue()
		self.__update_queue = Queue()
		self.__scheduled_tasks_queue = Queue()

		self.jobs = {}
		self.tasks_pool = []
		self.slots_free = {}
		self.worker_ports = {}

		self.scheduler = Scheduler(self, self.sched_policy)
		self.scheduler_lock = threading.Lock()

	def get_sched_polices(self) -> List[str]:
		return self.__sched_policies

	def config(self, path_to_config: str) -> object:
		with open(path_to_config, "r") as conf:
			self.worker_config = json.load(conf)

		for worker_info in self.worker_config["workers"]:
			worker_id = worker_info['worker_id']
			slots = worker_info["slots"]
			port = worker_info["port"]
			self.slots_free[worker_id] = slots
			self.worker_ports[worker_id] = port

		return self

	def initialize_connection(self):
		self.sender_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		for worker_info in self.worker_config["workers"]:

			worker_id = worker_info['worker_id']
			slots = worker_info["slots"]
			port = worker_info["port"]

			self.sender_socket.connect((self.master_ip, port))

			data = {
				'worker_slots_count': slots,
				'worker_id': worker_id
			}

			json_data = json.dumps(data)
			self.sender_socket.sendall(json_data)

			self.sender_socket.close()

		return self

	def __poll_job_queue(self) -> None:
		while True:
			client, job = self.__job_queue.get()
			logging.info("scheduling job: %s recieved from ip: %s port: %d"
						 % (job["job_id"], client[0], client[1]))
			self.__job_queue.task_done()

			job_id = job['job_id']
			raw_map_tasks = job['job_id']['map_tasks']
			raw_reduce_tasks = job['job_id']['reduce_tasks']

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

			job_object = {
				'total_map_tasks': len(map_tasks),
				'scheduled_map_tasks': 0,
				'completed_map_tasks': 0,
				'total_reduce_tasks': len(reduce_tasks),
				'scheduled_reduce_tasks': 0,
				'completed_reduce_tasks': 0,
				'reduce_tasks': reduce_tasks
			}

			tasks_to_be_sent = None

			with self.scheduler_lock:
				self.jobs[job[job_id]] = job_object
				self.tasks_pool.extend(map_tasks)
				tasks_to_be_sent = self.scheduler.schedule_tasks()

			for task in tasks_to_be_sent:
				__scheduled_tasks_queue.put(task)

			

	def __poll_update_queue(self) -> None:
		while True:
			worker, update = self.__update_queue.get()
			logging.info("received update from %s at ip: %s port: %d"
						 % (update["worker_id"], worker[0], worker[1]))

			self.__update_queue.task_done()

			worker_id = update['worker_id']
			tasks = update['data']

			with self.scheduler_lock:
				self.slots_free[worker_id] += len(tasks)

			completed_jobs = []

			for task in tasks:

				job_id = task['job_id']
				task_type = task['type']

				with self.scheduler_lock:
					if task_type == 'map':
						self.jobs[job_id]['completed_map_tasks'] += 1
						if self.jobs[job_id]['completed_map_tasks'] == \
							self.jobs[job_id]['total_map_tasks']:
							self.tasks_pool.extend(self.jobs[job_id]['total_reduce_tasks'])
					else:
						self.jobs[job_id]['completed_reduce_tasks'] += 1
						if self.jobs[job_id]['completed_reduce_tasks'] == \
							self.jobs[job_id]['total_reduce_tasks']:
							completed_jobs.append(job_id)
			
			# TODO: Log and Remove these job entries

			tasks_to_be_sent = None
			with self.scheduler_lock:
				tasks_to_be_sent = self.scheduler.schedule_tasks()

			for task in tasks_to_be_sent:
				__scheduled_tasks_queue.put(task)
							
	def send_tasks_requests(self):
		while True:
			task = self.__scheduled_tasks_queue.get()

			worker_id = task["worker_id"]
			port = self.worker_ports[worker_id]

			self.sender_socket.connect((self.master_ip, port))

			json_data = json.dumps(task)
			self.sender_socket.sendall(json_data)

			self.sender_socket.close()

	def set_sched_policy(self, sched_policy: str = "LL") -> object:
		self.sched_policy = sched_policy
		self.scheduler = Scheduler(self.sched_policy)
		return self

	def set_master_ip(self, ip: str = "127.0.0.1") -> object:
		self.master_ip = ip
		return self

	def start(self) -> None:
		try:
			job_listener = self.__spawn(
				self.__spawn_job_listener, args=(5000,))
			logging.info("job listener spawn successful")

			update_listener = self.__spawn(
				self.__spawn_update_listener, args=(5001,))
			logging.info("update listener spawn successful")

			job_queue_poll = self.__spawn(self.__poll_job_queue)
			logging.info("job queue spawn successful")

			job_listener.join()
			update_listener.join()
			job_queue_poll.join()

		except ThreadTerminate:
			logging.info("shutting down listeners")
			job_listeners = list(self.__job_listeners.values())
			update_listeners = list(self.__update_listeners.values())

			for listener in job_listeners + update_listeners:
				if not listener.shutdown_flag.is_set():
					listener.shutdown_flag.set()

	def __send_msg(self, addr, listeners) -> None:
		listeners[addr].ack_flag.set()

	def __spawn(self, func, args: tuple = ()) -> threading.Thread:
		thread = threading.Thread(target=func, args=args)
		thread.daemon = True
		thread.start()

		return thread

	def __spawn_job_listener(self, port: int = 5000) -> None:
		job_socket = socket.create_server((self.master_ip, port))
		job_socket.listen(1)

		while True:
			(client, client_addr) = job_socket.accept()
			logging.info("new incoming client connection, ip: %s port: %d" % (
				client_addr[0], client_addr[1]))

			job_listener = Listener(
				client, client_addr, "JOB_LISTENER", self.__job_queue)
			self.__job_listeners[client_addr] = job_listener
			job_listener.daemon = True
			job_listener.start()

	def __spawn_update_listener(self, port: int = 5001) -> None:
		update_socket = socket.create_server((self.master_ip, port))
		update_socket.listen(1)

		while True:
			(worker, worker_addr) = update_socket.accept()
			logging.info("new incoming worker connection, ip: %s port: %d" % (
				worker_addr[0], worker_addr[1]))

			update_listener = Listener(worker, worker_addr, "UPDATE_LISTENER")
			update_listener.daemon = True
			self.__update_listeners[worker_addr] = update_listener
			update_listener.start()

			with self.scheduler_lock:
				pass

	def __register_signal_handlers(self, handler_map: dict) -> None:
		for sig, handler in handler_map.items():
			signal.signal(sig, handler)


if __name__ == '__main__':
	try:
		path = sys.argv[1]
		sched_policy = sys.argv[2]
		logging.basicConfig(
			filename="yacs.log",
			format="%(levelname)s %(asctime)s: %(message)s",
			datefmt="%m/%d/%Y %I:%M:%S %p",
			level=logging.DEBUG,
		)
		m = Master()\
			.config(path_to_config=path)\
			.set_sched_policy(sched_policy=sched_policy)\
			.initialize_connection()
		m.start()

	except Exception as e:
		logging.error(e)

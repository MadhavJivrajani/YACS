import sys
import socket
import time
import json
import signal
import logging

from copy import deepcopy
from queue import Queue
from threading import Thread, Lock

sys.path.append("./")
sys.path.append("../../")

from yacs.utils.errors import *

__all__ = ['Worker']

class Worker:
	"""Logic that implements a worker node in the cluster. 

	It handles the following:

	* Listen for task submissions from the ``Master`` on ``port``
	* Simulate execution of tasks and send updates to ``Master`` on port ``5001``

	:param port: port on which the ``Worker`` listens for incoming tasks.
	:type port: `int`

	:param worker_id: unique identifier for the worker.
	:type port: `str`

	:param host: ip address of the ``Master`` node.
	:type host: `str`

	:param master_port: port of ``Master`` to which the ``Worker`` should send updates on.
	:type master_port: `int`
	"""
	def __init__(self,
		port: int,
		worker_id: str,
		host: str='127.0.0.1',
		master_port: int=5001,
	) -> None:

		self.host = host
		self.port = port
		self.master_port = master_port
		self.pool = []
		self.pool_edit_lock = Lock()
		self.worker_id = worker_id

		self.__completed_pool = Queue()

		# register signal handlers
		self.__register_signal_handlers({
			signal.SIGTERM	: handle_thread_terminate,
			signal.SIGINT	: handle_thread_terminate,
		})

	def start_worker(self) -> None:
		"""Start the worker by spawning threads for the following:
	
		* Listen for incoming tasks from the ``Master``
		* Send updates of completed tasks to ``Master``
		* Simulate execution of tasks

		For more information on this please refer :ref:`worker`
		"""
		try:
			listener_thread = Thread(target=self.__listen_for_task_requests)
			listener_thread.daemon = True
			task_thread = Thread(target=self.__execute_tasks)
			task_thread.daemon = True
			update_thread = Thread(target=self.__send_completion_update)
			update_thread.daemon = True

			listener_thread.start()
			task_thread.start()
			update_thread.start()

			listener_thread.join()
			task_thread.join()

			update_thread.join()
		except ThreadTerminate:
			self.listener_socket.close()
			logging.info("shutting down worker")

	# register signal handlers for graceful termination
	def __register_signal_handlers(self, handler_map: dict) -> None:
		for sig, handler in handler_map.items():
			signal.signal(sig, handler)

	# listen for tasks coming in from the Master
	def __listen_for_task_requests(self) -> None:
		while True:
			connection, _ = self.listener_socket.accept()
			data = ''
			with connection:
				while True:
					received_data = connection.recv(2048)
					# print(received_data)
					if not received_data:
						break
					data += received_data.decode('utf-8')

			new_task = eval(data)
			logging.info("task recieved: %s of job: %s" %
						 (new_task["task_id"], new_task["job_id"]))

			# update the shared task pool
			# data structure
			with self.pool_edit_lock:
				self.pool.append(new_task)

	# simulate execution of tasks
	def __execute_tasks(self) -> None:
		while True:
			# decrease duration of each task by 1
			num_tasks = len(self.pool)
			for i in range(num_tasks):
				self.pool[i]['duration'] -= 1

			new_pool = []
			completed_pool = []

			for i in range(num_tasks):
				if self.pool[i]['duration'] <= 0:
					completed_pool.append(self.pool[i])
					continue
				new_pool.append(self.pool[i])

			with self.pool_edit_lock:
				self.pool = deepcopy(new_pool)

			time.sleep(1)

			# add the completed tasks to the completed queue
			# this queue is polled by another thread to send
			# updates to the Master
			list(map(self.__completed_pool.put, completed_pool))

	# update the Master of completed tasks
	def __send_completion_update(self) -> None:
		while True:
			completed_tasks = []

			# form the update
			while not self.__completed_pool.empty():
				completed_task = self.__completed_pool.get()
				logging.info("task completed: %s of job: %s"
							 % (completed_task["task_id"], completed_task["job_id"]))
				self.__completed_pool.task_done()
				completed_tasks.append(completed_task)

			if len(completed_tasks) == 0:
				time.sleep(1)
				continue
			update_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			update_socket.connect((self.host, self.master_port))
			update = {
				'worker_id': self.worker_id,
				'data': completed_tasks,
			}
			json_data = json.dumps(update)

			# send the update to the Master
			update_socket.sendall(json_data.encode("utf-8"))
			update_socket.close()
			time.sleep(1)

	def initialize_connection(self) -> None:
		"""Create a socket to listen for task submissions
		from the ``Master``
		"""
		self.listener_socket = socket.socket(
			socket.AF_INET, socket.SOCK_STREAM)
		self.listener_socket.bind((self.host, self.port))

		self.listener_socket.listen(1)


if __name__ == '__main__':
	port = (int)(sys.argv[1])
	worker_id = sys.argv[2]

	logging.basicConfig(
		filename="worker_%s.log" % worker_id,
		format="%(levelname)s %(asctime)s.%(msecs)03d: %(message)s",
		datefmt="%m/%d/%Y %H:%M:%S",
		level=logging.DEBUG,
	)

	worker = Worker(port, worker_id)
	worker.initialize_connection()
	worker.start_worker()

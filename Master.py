import sys
import json
import threading
import socket
import signal
from typing import List
from queue import Queue

from yacs.component.listener import Listener
from yacs.utils.errors import handle_thread_terminate, ThreadTerminate

__all__ = ['Master']

class Master:
	def __init__(self) -> None:
		self.__sched_policies = ["LL", "RR", "R"]
		self.sched_policy = "LL"

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

		# poll frquency in ms
		self.__poll_freq = 100

	def get_sched_polices(self) -> List[str]:
		return self.__sched_policies

	def config(self, path_to_config: str) -> object:
		with open(path_to_config, "r") as conf:
			self.worker_config = json.load(conf)
		return self

	def __poll_job_queue(self) -> None:
		while True:
			job = self.__job_queue.get()
			# trigger scheduling
			print(job)
			self.__job_queue.task_done()

	def __poll_update_queue(self) -> None:
		while True:
			update = self.__update_queue.get()
			#trigger scheduling 
			print(update)
			self.__update_queue.task_done()

	def set_poll_frequency(self, freq: int=100) -> object:
		self.__poll_freq = freq
	
	def set_sched_policy(self, sched_policy: str = "LL") -> object:
		self.sched_policy = sched_policy
		return self
	
	def set_master_ip(self, ip: str = "127.0.0.1") -> object:
		self.master_ip = ip
		return self

	def start(self) -> None:
		# TODO: log
		try:
			job_listener = self.__spawn(self.__spawn_job_listener, args=(5000,))
			update_listener = self.__spawn(self.__spawn_update_listener, args=(5001,))

			job_queue_poll = self.__spawn(self.__poll_job_queue)

			job_listener.join()
			update_listener.join()
			job_queue_poll.join()

		# TODO: log
		except ThreadTerminate:
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
			job_listener = Listener(client, client_addr, "JOB_LISTENER", self.__job_queue)
			self.__job_listeners[client_addr] = job_listener
			job_listener.daemon = True
			job_listener.start()
	
	def __spawn_update_listener(self, port: int = 5001) -> None:
		update_socket = socket.create_server((self.master_ip, port))
		update_socket.listen(1)

		while True:			
			(worker, worker_addr) = update_socket.accept()
			update_listener = Listener(worker, worker_addr, "UPDATE_LISTENER")
			update_listener.daemon = True
			self.__update_listeners[worker_addr] = update_listener
			update_listener.start()
	
	def __register_signal_handlers(self, handler_map: dict) -> None:
		for sig, handler in handler_map.items():
			signal.signal(sig, handler)

if __name__ == '__main__':
	try:
		path = sys.argv[1]
		sched_policy = sys.argv[2]

		m = Master()\
			.config(path_to_config=path)\
			.set_sched_policy(sched_policy=sched_policy)
		m.start()
		
	except Exception as e:
		print(e)
	# TODO: log this

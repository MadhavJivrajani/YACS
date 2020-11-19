import json
import socket
import threading

__all__ = ['Listener']

class Listener(threading.Thread):
	def __init__(self,
				sock,
				client_addr: tuple,
				name: str = "",
				buff_size: int = 2048,
				) -> None:
		threading.Thread.__init__(self)
		self.__name = name
		self.__client = sock
		self.buff_size = buff_size
		
		self.__client_addr = client_addr # for logging

		self.recv_json = {}

		self.shutdown_flag = threading.Event()

	# TODO: logging
	def run(self) -> None:
		while not self.shutdown_flag.is_set():
			data = self.__client.recv(self.buff_size)
			if not data:
				break
			data = data.decode("utf-8")
			self.recv_json = json.loads(data)
			print(self.__name, self.recv_json)

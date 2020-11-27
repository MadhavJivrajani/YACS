import json
import socket
import threading
import logging

from queue import Queue

__all__ = ['Listener']

class Listener(threading.Thread):
	"""Implements an abstraction for a connection to
	either a client or a worker sending updates. A
	``Listener`` inherits from the ``threading.Thread`` class
	and maintains each connection in a seperate thread.

	:param sock: socket of the incoming connection
	:type sock: :py:class:`socket.socket`

	:param client_addr: (ip, port) of the incoming connection
	:type client_addr: `tuple`

	:param queue: shared queue to put received jobs/updates. For more information on this, refer :ref:`dev`.
	:type queue: :py:class:`queue.Queue`

	:param name: name of the type of listener (``JOB_LISTENER``, ``UDPATE_LISTENER``)
	:type name: `str`

	:param buff_size: maximum chunk of data that can be recieved at once.
	:type buff_size: `int`
	"""
	def __init__(self,
		sock: socket.socket,
		client_addr: tuple,
		queue: Queue,
		name: str = "",
		buff_size: int = 2048,
	) -> None:

		threading.Thread.__init__(self)
		self.__name = name
		self.__client = sock
		self.buff_size = buff_size
		
		self.__client_addr = client_addr # for logging

		self.recv_json = {}

		self.ack_flag = threading.Event()

		self.__queue = queue

	def run(self) -> None:
		"""Recieves data and enqueues it in its appropriate ``queue``."""
		data = []

		# recieve data in chunks
		while True:
			data_chunk = self.__client.recv(self.buff_size)
			if not data_chunk:
				break
			data.append(data_chunk.decode("utf-8"))

		data = ''.join(data)
		if self.ack_flag.is_set():
			self.ack_flag.clear()
			self.__client.send(b"ack-test")

		self.recv_json = json.loads(data)

		# enqueue data along with the address of the machine that sent it
		# address if of the form (ip, port)
		self.__queue.put((self.__client_addr, self.recv_json))

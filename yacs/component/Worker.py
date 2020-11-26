import sys
import socket
import time
import json
from copy import deepcopy
from queue import Queue
from threading import Thread, Lock

class Worker:
	def __init__(self, PORT, worker_id: str, HOST='127.0.0.1', MASTER_PORT=5001):

		self.HOST = HOST
		self.PORT = PORT
		self.MASTER_PORT = MASTER_PORT
		self.pool = []
		self.pool_edit_lock = Lock()
		self.worker_id = worker_id

		self.__completed_pool = Queue()

	def start_worker(self):
		
		listener_thread = Thread(target = self.listen_for_task_requests)
		listener_thread.daemon = True
		task_thread = Thread(target = self.execute_tasks)
		task_thread.daemon = True
		update_thread = Thread(target = self.send_completion_requests)
		update_thread.daemon = True

		listener_thread.start()
		task_thread.start()
		update_thread.start()

		listener_thread.join()
		task_thread.join()

		update_thread.join()

	def listen_for_task_requests(self):
		while True:
			connection, _ = self.listener_socket.accept()
			data = ''
			with connection:
				while True:
					received_data = connection.recv(2048)
					#print(received_data)
					if not received_data:
						break
					data += received_data.decode('utf-8')

			new_task = eval(data)
			print("TASK RECV: %s" % new_task["task_id"])
			with self.pool_edit_lock:
				self.pool.append(new_task)

	def execute_tasks(self):
		while True:
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
			list(map(self.__completed_pool.put, completed_pool))

	def send_completion_requests(self):
		while True:
			completed_tasks = []
			while not self.__completed_pool.empty():
				completed_task = self.__completed_pool.get()
				print("TASK COMP: %s" % completed_task["task_id"])

				#print(completed_task)
				self.__completed_pool.task_done()
				completed_tasks.append(completed_task)

			if len(completed_tasks) == 0:
				time.sleep(1)
				continue
			update_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			update_socket.connect((self.HOST, self.MASTER_PORT))
			data = {
				'worker_id': self.worker_id,
				'data': completed_tasks,
			}
			json_data = json.dumps(data)
			update_socket.sendall(json_data.encode("utf-8"))
			update_socket.close()
			time.sleep(1)

	def initialize_connection(self):
		self.listener_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.listener_socket.bind((self.HOST, self.PORT))

		self.listener_socket.listen(1)

if __name__ == '__main__':
	PORT = (int)(sys.argv[1])
	worker_id = sys.argv[2]
	worker = Worker(PORT, worker_id)
	worker.initialize_connection()
	worker.start_worker()

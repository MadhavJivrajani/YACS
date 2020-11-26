import random
import time
import logging

class Random:

	def __init__(self, master: object) -> None:
		self.master = master

	# Returns True only when all tasks are scheduled
	def is_buffer_empty(self) -> bool:
		return self.master.tasks_pool.empty()

	def schedule_tasks(self) -> None:
		# Iterate until the all jobs are not scheduled
		while not self.is_buffer_empty():
			is_slot_free = True	
			with self.master.scheduler_lock:
				# Get the worker id that is least loaded
				worker_id_list = list(self.master.slots_free.keys())
				worker_id = random.choice(worker_id_list)
				if self.master.slots_free[worker_id] == 0:
					is_slot_free = False

			if not is_slot_free:
				time.sleep(1)
				continue

			with self.master.scheduler_lock:
				#logging.info("Scheduler second lock acquired")	
				next_task = self.master.tasks_pool.get()

				logging.info("task %s from job %s scheduled on %s" \
					% (next_task["task_id"], next_task["job_id"], worker_id))
				next_task["worker_id"] = worker_id

				self.master.scheduled_tasks_queue.put(next_task)
				self.master.slots_free[worker_id] -= 1

class RoundRobin:

	def __init__(self, master: object) -> None:
		self.master = master

		self.workers = list(self.master.slots_free.keys())
		self.num_workers = len(self.workers)
		self.curr_index = -1

	# Returns True only when all tasks are scheduled
	def is_buffer_empty(self) -> bool:
		return self.master.tasks_pool.empty()

	def schedule_tasks(self) -> None:
		# Iterate until the all jobs are not scheduled
		while not self.is_buffer_empty():
			is_slot_free = True	
			with self.master.scheduler_lock:
				# Get the worker id that is least loaded
				self.curr_index = (self.curr_index + 1) % self.num_workers
				worker_id = self.workers[self.curr_index]
				if self.master.slots_free[worker_id] == 0:
					is_slot_free = False

			if not is_slot_free:
				time.sleep(1)
				continue

			with self.master.scheduler_lock:
				#logging.info("Scheduler second lock acquired")	
				next_task = self.master.tasks_pool.get()

				logging.info("task %s from job %s scheduled on %s" \
					% (next_task["task_id"], next_task["job_id"], worker_id))
				next_task["worker_id"] = worker_id

				self.master.scheduled_tasks_queue.put(next_task)
				self.master.slots_free[worker_id] -= 1

# task pool is empty or no free slots
class LeastLoaded:
	def __init__(self, master: object) -> None:
		self.master = master

		"""
		Ex: buffer
			[[job_id, 1], [job_id, 0], [job_id, 0], ...]
		"""
		#self.curr_jobs_in_seq = 
		"""
		Ex: curr_jobs_in_seq
			[job_id_0, job_id_1, job_id_2, ....]
		"""


	# Returns True only when all tasks are scheduled
	def is_buffer_empty(self) -> bool:
		return self.master.tasks_pool.empty()
	
	# Returns a list of job_ids that are not scheduled
	def __get_unscheduled_jobs(self) -> list:
		return self.master.tasks_pool

	# Returns the worker_id having that is least loaded i.e. having most number of free slots
	def __get_least_loaded_worker_id(self) -> str:
	   return max(self.master.slots_free, key= lambda x: self.master.slots_free[x])

	def __schedule_dummy_print(self, job_id, task_type, worker_id) -> None:
		print(worker_id, '\tjob_id', job_id, '\t', task_type)

	# Least Loaded Scheduling Algorithm

	def schedule_tasks(self) -> None:
		# Iterate until the all jobs are not scheduled
		while not self.is_buffer_empty():
			is_slot_free = True			
			with self.master.scheduler_lock:
				# Get the worker id that is least loaded
				worker_id = self.__get_least_loaded_worker_id()
				if self.master.slots_free[worker_id] == 0:
					is_slot_free = False

			if not is_slot_free:
				time.sleep(1)
				continue

			with self.master.scheduler_lock:
				#logging.info("Scheduler second lock acquired")	
				next_task = self.master.tasks_pool.get()

				logging.info("task %s from job %s scheduled on %s" \
					% (next_task["task_id"], next_task["job_id"], worker_id))
				next_task["worker_id"] = worker_id

				self.master.scheduled_tasks_queue.put(next_task)
				self.master.slots_free[worker_id] -= 1

class Scheduler:
	def __init__(self, master: object, strategy: str = "LL") -> None:

		self.master = master		
		self.strategy = strategy

		self.scheduler = self.__sched_class()

	def __sched_class(self) -> object:
		if self.strategy == "LL":
			return LeastLoaded(self.master)
		elif self.strategy == "R":
			return Random(self.master)
		elif self.strategy == "RR":
			return RoundRobin(self.master)

	def schedule_tasks(self):
		return self.scheduler.schedule_tasks()

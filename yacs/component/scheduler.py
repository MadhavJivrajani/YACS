import random
import time
import logging

__all__ = ['Random', 'LeastLoaded', 'RoundRobin', 'Scheduler']

class Random:
	"""Class implementing logic for the Random (``R``) scheduling policy.

	A worker machine is picked at random and checked for availability of free slots
	If slots are available, a task is scheduled and subsequently launched on this 
	machine, if no free slots are found then it chooses another machine at random 
	and this process continues till a free slot is found. 

	:param master: reference to the ``Master`` object to access and modify relevant data structures
	:type master: :py:class:`Master`
	"""
	def __init__(self, master: object) -> None:
		self.master = master

	def is_buffer_empty(self) -> bool:
		"""Check if task pool is empty"""
		return self.master.tasks_pool.empty()

	def schedule_tasks(self) -> None:
		"""Schedule tasks from the task pool according to
		the Random (``R``) policy. This will run till all
		the tasks in the buffer have been scheduled. 
		"""
		# Iterate until the all jobs are not scheduled
		while not self.is_buffer_empty():
			is_slot_free = True	
			with self.master.scheduler_lock:
				# pick a random worker from the list of workers
				worker_id_list = list(self.master.slots_free.keys())
				worker_id = random.choice(worker_id_list)
				if self.master.slots_free[worker_id] == 0:
					is_slot_free = False

			if not is_slot_free:
				time.sleep(1)
				continue

			# update relevant data structures
			with self.master.scheduler_lock:
				next_task = self.master.tasks_pool.get()

				logging.info("task %s from job %s scheduled on %s" \
					% (next_task["task_id"], next_task["job_id"], worker_id))
				next_task["worker_id"] = worker_id

				self.master.scheduled_tasks_queue.put(next_task)
				self.master.slots_free[worker_id] -= 1

class RoundRobin:
	"""Class implementing logic for the Round Robin (``RR``) scheduling policy

	The worker machines are ordered based on the Worker IDs. A worker is picked in
	a round robin fashion and if the worker does not have free slots then the next
	worker is picked. This continues till a worker with free slots is found.

	:param master: reference to the ``Master`` object to access and modify relevant data structures
	:type master: :py:class:`Master`
	"""
	def __init__(self, master: object) -> None:
		self.master = master

		# order by worker IDs
		self.workers = sorted(list(self.master.slots_free.keys()))
		self.num_workers = len(self.workers)
		self.curr_index = -1

	def is_buffer_empty(self) -> bool:
		"""Check if task pool is empty"""
		return self.master.tasks_pool.empty()

	def schedule_tasks(self) -> None:
		"""Schedule tasks from the task pool according to
		the Round Robin (``RR``) policy. This will run till 
		all the tasks in the buffer have been scheduled. 
		"""
		# Iterate until the all jobs are not scheduled
		while not self.is_buffer_empty():
			is_slot_free = True	
			with self.master.scheduler_lock:
				# get the next worker in the list of workers in
				# a circular fashion
				self.curr_index = (self.curr_index + 1) % self.num_workers
				worker_id = self.workers[self.curr_index]
				if self.master.slots_free[worker_id] == 0:
					is_slot_free = False

			# sleep only if slots are not free
			# done to make sure that the lock is not held 
			# during sleep
			if not is_slot_free:
				time.sleep(1)
				continue

			# update relevant data structures
			with self.master.scheduler_lock:
				next_task = self.master.tasks_pool.get()

				logging.info("task %s from job %s scheduled on %s" \
					% (next_task["task_id"], next_task["job_id"], worker_id))
				next_task["worker_id"] = worker_id

				self.master.scheduled_tasks_queue.put(next_task)
				self.master.slots_free[worker_id] -= 1

class LeastLoaded:
	"""Class implementing logic for the Least Loaded (``LL``) scheduling policy

	The state of all workers is checked and the worker with *most number of free slots*
	is selected and a task is scheduled and subsequently launched there. If no worker
	has free slots available then the scheduler will wait for 1 second and repeats the
	same process again.

	:param master: reference to the ``Master`` object to access and modify relevant data structures
	:type master: :py:class:`Master`
	"""
	def __init__(self, master: object) -> None:
		self.master = master

	def is_buffer_empty(self) -> bool:
		"""Check if task pool is empty"""
		return self.master.tasks_pool.empty()
	
	# Returns a list of job_ids that are not scheduled
	def __get_unscheduled_jobs(self) -> list:
		return self.master.tasks_pool

	# Returns the worker_id having that is least loaded
	# i.e. having most number of free slots
	def __get_least_loaded_worker_id(self) -> str:
	   return max(self.master.slots_free, key= lambda x: self.master.slots_free[x])

	def __schedule_dummy_print(self, job_id, task_type, worker_id) -> None:
		print(worker_id, '\tjob_id', job_id, '\t', task_type)

	def schedule_tasks(self) -> None:
		"""Schedule tasks from the task pool according to
		the Least Loaded (``LL``) policy. This will run till 
		all the tasks in the buffer have been scheduled. 
		"""
		while not self.is_buffer_empty():
			is_slot_free = True			
			with self.master.scheduler_lock:
				# Get the worker id that is least loaded
				worker_id = self.__get_least_loaded_worker_id()
				if self.master.slots_free[worker_id] == 0:
					is_slot_free = False

			# sleep only if slots are not free
			# done to make sure that the lock is not held 
			# during sleep
			if not is_slot_free:
				time.sleep(1)
				continue

			# update relevant data structures
			with self.master.scheduler_lock:
				#logging.info("Scheduler second lock acquired")	
				next_task = self.master.tasks_pool.get()

				logging.info("task %s from job %s scheduled on %s" \
					% (next_task["task_id"], next_task["job_id"], worker_id))
				next_task["worker_id"] = worker_id

				self.master.scheduled_tasks_queue.put(next_task)
				self.master.slots_free[worker_id] -= 1

class Scheduler:
	"""A single point of access for scheduling needs, acts as a wrapper 
	around the three scheduling policies.

	:param master: reference to the ``Master`` object to access and modify relevant data structures
	:type master: :py:class:`Master`

	:param policy: scheduling policy to use 
	:type master: `str`
	"""
	def __init__(self, master: object, policy: str = "LL") -> None:

		self.master = master		
		self.policy = policy

		self.scheduler = self.__sched_class()

	# convenience function to instantiate one of the three
	# scheduling objects and return it
	def __sched_class(self) -> object:
		if self.policy == "LL":
			return LeastLoaded(self.master)
		elif self.policy == "R":
			return Random(self.master)
		elif self.policy == "RR":
			return RoundRobin(self.master)

	def schedule_tasks(self) -> None:
		"""Interface for triggering scheduling
		functionality of the respective scheduler
		based on the scheduling policy used
		"""
		self.scheduler.schedule_tasks()

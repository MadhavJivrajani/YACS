import random
import time

class Random:
	def __init__(self, master: object) -> None:
		self.master = master
		self.workers = list(self.master.worker_config["workers"]\
			.map(lambda x: x["worker_id"]))

	def __is_slot_available(self, worker_id: str) -> bool:
		return self.master.slots_free[worker_id] != 0
	
	def __choose_worker(self) -> str:
		return random.choice(self.workers)

	def schedule_task(self) -> None:
		worker = None
		while True:
			worker = self.__choose_worker()
			if self.__is_slot_available(worker):
				break
		to_sched = self.master.task_pool.pop(0)
		print(to_sched)

class LeastLoaded:
	def __init__(self, master: object) -> None:
		self.master = master

		self.buffer = list()
		"""
		Ex: buffer
			[[job_id, 1], [job_id, 0], [job_id, 0], ...]
		"""
		self.curr_jobs_in_seq = list()
		"""
		Ex: curr_jobs_in_seq
			[job_id_0, job_id_1, job_id_2, ....]
		"""

	# Returns True only when all tasks are scheduled
	def is_buffer_empty(self) -> bool:

		for job in self.buffer:
			if job[1] == 0:
				return False

		# Clear buffer when all tasks are scheduled
		self.buffer = list()
		return True


	# Appends scheduling status of jobs to buffer
	def buffer_append_job_tasks(self, job_id) -> None:
		self.buffer.append([job_id, 0])


	# Makes the curr job status in the buffer as all scheduled
	def buffer_remove_job_tasks(self, job_id) -> None:
		for job_index in range(len(self.curr_jobs_in_seq)):
			if job_id == self.buffer[job_index][0]:
				self.buffer[job_index][1] = 1
				return


	# Returns a list of job_ids that are not scheduled
	def __get_unscheduled_jobs(self) -> list:
		return [job[0] for job in self.buffer if job[1] == 0]

	"""
	def __get_least_loaded_worker_order(self) -> list:
		#return list(sorted(worker_free_slots.items(), key = lambda k: k[1], reverse = True))
		return [worker[0] for worker in \
				sorted(worker_free_slots.items(), key = lambda k: k[1], reverse = True)]
	"""


	# Returns the worker_id having that is least loaded i.e. having most number of free slots
	def __get_least_loaded_worker_id(self) -> str:
	   return max(self.master.slots_free, key= lambda x: self.master.slots_free[x])


	# Least Loaded Scheduling Algorithm
	def schedule_tasks(self) -> None:

		# Iterate until the all jobs are not scheduled
		while not self.is_buffer_empty():

			# Get the current state of jobs that are not scheduled
			self.curr_jobs_in_seq = self.__get_unscheduled_jobs()

			# For each job in current state of jobs that are not scheduled
			for job in self.curr_jobs_in_seq:

				# Schedule all map tasks of a particular job
				while (self.master.jobs[job]['scheduled_map_tasks'] < self.master.jobs[job]['total_map_tasks']):

					# Get the worker id that is least loaded
					worker_id = self.__get_least_loaded_worker_id()

					# If none of the workers are free, wait for 1 second
					if (worker_id == 0):
						time.sleep(1)

					# Schedule the map task to the least loaded worker
					else:
						self.master.jobs[job]['scheduled_map_tasks'] += 1
						#schedule_dummy_print(job, 'map_task', worker_id)

				# check dependecy: whether all the map tasks are completed before scheduling reduce tasks
				if self.master.jobs[job]['completed_map_tasks'] == self.master.jobs[job]['total_map_tasks']:

					# Schedule all reduce tasks of a particular job
					while (self.master.jobs[job]['scheduled_reduce_tasks'] < self.master.jobs[job]['total_reduce_tasks']):

						# Get the worker id that is least loaded
						worker_id = self.__get_least_loaded_worker_id()

						# If none of the workers are free, wait for 1 second
						if (worker_id == 0):
							time.sleep(1)

						# Schedule the reduce task to the least loaded worker
						else:
							self.master.jobs[job]['scheduled_reduce_tasks'] += 1
							#schedule_dummy_print(job, 'reduce_task', worker_id)


					# When all tasks of a job is complete, makes the curr job status in the buffer as all scheduled
					if self.master.jobs[job]['completed_reduce_tasks'] == self.master.jobs[job]['total_reduce_tasks']:
						self.buffer_remove_job_tasks(job)
						break

				# When the map tasks of the curr job is not complete, it starts scheduling the next job's map tasks.
				# curr_job_in_seq is again called and before the next map tasks is about to be scheduled,
				# it checks whether the previous map tasks are completed & schedules the previous reduce tasks
				else:
					break

class Scheduler:
	def __init__(self, master: object, strategy: str = "LL") -> None:

		self.master = master		
		self.strategy = strategy

		self.scheduler = self.__sched_class()

	def __sched_class(self) -> object:
		if self.strategy == "LL":
			return LeastLoaded(self.master)

	def schedule_tasks(self):
		return self.scheduler.schedule_tasks()

import time
import random

job_status = dict()
"""
Ex: job_status
    {
    '0': {'total_map_tasks': 1, 'scheduled_map_tasks': 0, 'completed_map_tasks': 0, \
           'total_reduce_tasks': 2, 'scheduled_reduce_tasks': 0, 'completed_reduce_tasks': 0},

    }
"""

worker_free_slots = dict()
"""
Ex: worker_free_slots
    {'worker_id_1': 5, 'worker_id_2': 7, 'worker_id_3': 3, ...}
"""
worker_free_slots['worker_id_1'] = 5
worker_free_slots['worker_id_2'] = 7
worker_free_slots['worker_id_3'] = 3

def create_job_request(job_id):
	number_of_map_tasks=random.randrange(1,5)
	number_of_reduce_tasks=random.randrange(1,3)
	job_request={"job_id":job_id,"map_tasks":[],"reduce_tasks":[]}
	for i in range(0,number_of_map_tasks):
		map_task={"task_id":job_id+"_M"+str(i),"duration":random.randrange(1,5)}
		job_request["map_tasks"].append(map_task)
	for i in range(0,number_of_reduce_tasks):
		reduce_task={"task_id":job_id+"_R"+str(i),"duration":random.randrange(1,5)}
		job_request["reduce_tasks"].append(reduce_task)
	return job_request

def schedule_dummy_print(job_id, task_type, worker_id) -> None:
    print(worker_id, '\tjob_id', job_id, '\t', task_type)
    worker_free_slots[worker_id] -= 1
    if task_type == 'map_task':
        job_status[job_id]['completed_map_tasks'] += 1
        worker_free_slots[worker_id] += 1
    else:
        job_status[job_id]['completed_reduce_tasks'] += 1
        worker_free_slots[worker_id] += 1


# Append request information into job_status dictionary
def append_to_job_status(request) -> None:

    job_status[request['job_id']] = dict()

    job_status[request['job_id']]['total_map_tasks'] =  len(request['map_tasks'])
    job_status[request['job_id']]['scheduled_map_tasks'] = 0
    job_status[request['job_id']]['completed_map_tasks'] = 0

    job_status[request['job_id']]['total_reduce_tasks'] = len(request['reduce_tasks'])
    job_status[request['job_id']]['scheduled_reduce_tasks'] = 0
    job_status[request['job_id']]['completed_reduce_tasks'] = 0





class Least_Loaded_Scheduler:

    def __init__(self):

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
       return max(worker_free_slots, key= lambda x: worker_free_slots[x])


    # Least Loaded Scheduling Algorithm
    def schedule_tasks(self) -> None:

        # Iterate until the all jobs are not scheduled
        while not self.is_buffer_empty():

            # Get the current state of jobs that are not scheduled
            self.curr_jobs_in_seq = self.__get_unscheduled_jobs()

            # For each job in current state of jobs that are not scheduled
            for job in self.curr_jobs_in_seq:

                # Schedule all map tasks of a particular job
                while (job_status[job]['scheduled_map_tasks'] < job_status[job]['total_map_tasks']):

                    # Get the worker id that is least loaded
                    worker_id = self.__get_least_loaded_worker_id()

                    # If none of the workers are free, wait for 1 second
                    if (worker_id == 0):
                        time.sleep(1)

                    # Schedule the map task to the least loaded worker
                    else:
                        job_status[job]['scheduled_map_tasks'] += 1
                        schedule_dummy_print(job, 'map_task', worker_id)

                # check dependecy: whether all the map tasks are completed before scheduling reduce tasks
                if job_status[job]['completed_map_tasks'] == job_status[job]['total_map_tasks']:

                    # Schedule all reduce tasks of a particular job
                    while (job_status[job]['scheduled_reduce_tasks'] < job_status[job]['total_reduce_tasks']):

                        # Get the worker id that is least loaded
                        worker_id = self.__get_least_loaded_worker_id()

                        # If none of the workers are free, wait for 1 second
                        if (worker_id == 0):
                            time.sleep(1)

                        # Schedule the reduce task to the least loaded worker
                        else:
                            job_status[job]['scheduled_reduce_tasks'] += 1
                            schedule_dummy_print(job, 'reduce_task', worker_id)


                    # When all tasks of a job is complete, makes the curr job status in the buffer as all scheduled
                    if job_status[job]['completed_reduce_tasks'] == job_status[job]['total_reduce_tasks']:
                        self.buffer_remove_job_tasks(job)
                        break

                # When the map tasks of the curr job is not complete, it starts scheduling the next job's map tasks.
                # curr_job_in_seq is again called and before the next map tasks is about to be scheduled,
                # it checks whether the previous map tasks are completed & schedules the previous reduce tasks
                else:
                    break





if __name__ == "__main__":

    # Initialize type of scheduler
    LL_scheduler = Least_Loaded_Scheduler()

    for i in range(3):

        request = create_job_request(str(i))
        append_to_job_status(request)

        print('\n', job_status, '\n')

        # Append job id to buffer
        LL_scheduler.buffer_append_job_tasks(request['job_id'])

        # Scheule tasks in least loaded order
        LL_scheduler.schedule_tasks()

        print()
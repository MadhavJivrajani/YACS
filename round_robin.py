import json

requests = {'job_id': '0', 'map_tasks': [{'task_id': '0_M0', 'duration': 3}, {'task_id': '0_M1', 'duration': 2}], 'reduce_tasks': [{'task_id': '0_R0', 'duration': 1}, {'task_id': '0_R1', 'duration': 3}]}


#initialise all jobs and tasks to 0 before scheduling
job_status = dict()

curr=0

for job in requests:
    if job=='job_id':
        job_status[requests['job_id']] = dict()
        job_status[requests['job_id']]['map_tasks'] = dict()
        job_status[requests['job_id']]['reduce_tasks'] = dict()
        curr=requests['job_id']
    
    if job=='map_tasks':
        for task in requests['map_tasks']:
            job_status[curr]['map_tasks'][task['task_id']] = 0

    if job=='reduce_tasks':
        for task in requests['reduce_tasks']:
            job_status[curr]['reduce_tasks'][task['task_id']] = 0


#print initial job buffer to see
print(job_status)


#assume 3 workers for now, with random no. of slots. Ports not fixed here.
workers = ['worker_0','worker_1','worker_2']

slots = dict()
slots['worker_0'] = [x+1 for x in range(2)]
slots['worker_1'] = [x+1 for x in range(1)]
slots['worker_2'] = [x+1 for x in range(2)]

worker_status = dict()
worker_status['worker_0'] = []
worker_status['worker_1'] = []
worker_status['worker_2'] = []

for w in workers:
    worker_status[w] = []


#Round robin scheduling

#have a var/list to check if map tasks are completed before reduce tasks begin
map_done = []
red_done = []

for job in job_status:

    for m in job_status[job]['map_tasks']:
        status = job_status[job]['map_tasks'][m]
        if(status >=1 ):
            continue    #already scheduled or completed
        
        #now check each worker for empty slots when status=0(yet to be scheduled)
        n = len(workers)
        for i in range(n):

            w = workers[i]
            if len(slots[w])>0:
                job_status[job]['map_tasks'][m] = 1 #scheduled to the first empty slot
                slot_no = slots[w][0]
                slots[w].pop(0)
                worker_status[w] = [job,m]
                print("Job:",job," task:",m," worker:",w," slot no.:",slot_no)
                break
            
            if(i==n-1):
                i=0
            

    #check dependency over here
    #if len(map_done) = len(job_status[job]['map_tasks'])

    for r in job_status[job]['reduce_tasks']:
        status = job_status[job]['reduce_tasks'][r]
        if(status >=1 ):
            continue    #already scheduled or completed
        
        #now check each worker for empty slots when status=0(yet to be scheduled)
        n = len(workers)
        for i in range(n):

            w = workers[i]
            if len(slots[w])>0:
                job_status[job]['reduce_tasks'][r] = 1 #scheduled
                slot_no = slots[w][0]
                slots[w].pop(0)
                worker_status[w] = [job,r]
                print("Job:",job," task:",r," worker:",w," slot no.:",slot_no)
                break
            
            if(i==n-1):
                i=0


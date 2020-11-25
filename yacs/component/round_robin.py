import json

comp_req = {
        'worker_id':1,
        'data':{'map_tasks':['0_M1'],'reduce_tasks':[]},    #completed task IDs
        }

requests = {'job_id': '0', 'map_tasks': [{'task_id': '0_M0', 'duration': 3}, {'task_id': '0_M1', 'duration': 1},{'task_id': '0_M2','duration': 4}], 'reduce_tasks': [{'task_id': '0_R0', 'duration': 1}, {'task_id': '0_R1', 'duration': 3}]}

#initialise all jobs and tasks to 0 before scheduling

# {
#     job_id: {
#         total_map: 1,
#         ksker: 1,
#     }
# }

job_status = dict()
job_status['total_map_tasks'] = len(requests['map_tasks'])
job_status['map_done'] = 0
job_status['total_reduce_tasks'] = len(requests['reduce_tasks'])
job_status['red_done'] = 0

tasks_left = [] 
tasks_done = []

#based on completion request
tasks_done.extend(comp_req['data']['map_tasks'])
tasks_done.extend(comp_req['data']['reduce_tasks'])
job_status['map_done']+=len(comp_req['data']['map_tasks'])
job_status['red_done']+=len(comp_req['data']['reduce_tasks'])

print("Job status initially: ",job_status)

curr=0
#tasks_done.extend(comp_req['data'])

for job in requests:
    if job == 'map_tasks' or job=='reduce_tasks':
        for x in requests[job]:
            if x['task_id'] not in tasks_done:
                tasks_left.append((x['task_id']))


#print initial job buffer to see
#print(job_status)
print("tasks left: ",tasks_left)
print("tasks completed: ",tasks_done)
print("\n")

#assume 3 workers for now, with random no. of slots. Ports not fixed here.
workers = ['worker_0','worker_1','worker_2']

slots = dict()
slots['worker_0'] = [x+1 for x in range(2)]
slots['worker_1'] = [x+1 for x in range(1)]
slots['worker_2'] = [x+1 for x in range(2)]

#tasks being done by each worker
worker_status = dict()
worker_status['worker_0'] = []
worker_status['worker_1'] = []
worker_status['worker_2'] = []

for w in workers:
    worker_status[w] = []

#Round robin scheduling
def round_robin():
    
    for job in requests:

        for m in requests['map_tasks']:
            if m['task_id'] in tasks_left:

                for w in workers:
                    if(len(slots[w])==0):   #no empty slots
                        continue

                    worker_status[w].append(slots[w][0])
                    print(m['task_id']," ",w," slot:",slots[w][0])
                    slots[w].pop(0)
                    tasks_left.remove(m['task_id'])

                    #temporary - will increase it based on job completion requests from worker once added to master
                    
                    tasks_done.append(m['task_id'])
                    slots[w].append(worker_status[w][-1])
                    worker_status[w].pop()
                    job_status['map_done']+=1
                    break
                     

        if job_status['map_done'] == job_status['total_map_tasks']: #checking dependency
            for r in requests['reduce_tasks']:
                if r['task_id'] in tasks_left:

                    for w in workers:
                        if(len(slots[w])==0):   #no empty slots
                            continue

                        worker_status[w].append(slots[w][0])
                        print(r['task_id']," ",w," slot:",slots[w][0])
                        slots[w].pop(0)
                        tasks_left.remove(r['task_id'])
                        
                        tasks_done.append(r['task_id'])
                        slots[w].append(worker_status[w][-1])
                        worker_status[w].pop()
                        job_status['red_done']+=1
                        break



round_robin()
print("\nJob status after scheduling: ",job_status)

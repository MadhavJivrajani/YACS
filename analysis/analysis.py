import json
import numpy as np
from datetime import datetime as dt
import matplotlib.pyplot as plt

data = {}
keys = {
    'RR': 'Round Robin',
    'R': 'Random',
    'LL': 'Least Loaded'
}
time_format = "%Y-%m-%d %H:%M:%S.%f"
with open('data.json', 'r') as fp:
    data = json.load(fp)

for key in data:

    master_data = data[key]['master_data']
    job_times = []
    jobs = master_data['jobs']
    for job_obj in jobs:
        job_id = job_obj['job_id']
        arrival_time = dt.strptime(job_obj['arrival_time'], time_format)
        end_time = dt.strptime(job_obj['end_time'], time_format)
        job_times.append((end_time - arrival_time).total_seconds())
        
    job_times = np.array(job_times)
    print("%s: " % keys[key.upper()])
    print("\t Mean time of job completion: %.5f" % np.mean(job_times))
    print("\t Median time of job completion: %.5f" % np.median(job_times))
    

    worker_data = data[key]['worker_data']
    task_times = []
    
    for worker_id in worker_data:
        for task_obj in worker_data[worker_id]:    
            task_id = task_obj['task_id']
            arrival_time = dt.strptime(task_obj['arrival_time'], time_format)
            end_time = dt.strptime(task_obj['end_time'], time_format)
            task_times.append((end_time - arrival_time).total_seconds())
        
    task_times = np.array(task_times)
    print("\t Mean time of task completion: %.5f" % np.mean(task_times))
    print("\t Median time of task completion: %.5f" % np.median(task_times))

color_list = ['#4D0DD6', '#0BEF2A', '#085EFF', 'red', 'black', 'brown', 'purple']

def plot_slots(key):
    timestamps = data[key]['master_data']['tasks']
    num_timestamps = len(timestamps) - 1
    
    worker_ids = list(timestamps[0].keys())
    num_workers = len(worker_ids)
    
    timestamp_list = []
    plot_object = {}
    for worker_id in worker_ids:
        plot_object[worker_id] = []
                
    starting_time = dt.strptime(timestamps[1]['timestamp'], time_format)
        
    for timestamp in timestamps[1:]:
        timestamp_obj = dt.strptime(timestamp['timestamp'], time_format)
        timestamp_list.append((timestamp_obj - starting_time).total_seconds())
        
        for worker_id in worker_ids:
            plot_object[worker_id].append(timestamp[worker_id])
            
    for index in range(num_workers):
        plt.plot(timestamp_list, plot_object[worker_ids[index]], color = color_list[index], label="Worker %s" % worker_ids[index])
        plt.legend(loc = 'upper left')

    plt.title("Slots occupied Vs Time Elapsed: %s" % keys[key.upper()])
    plt.xlabel("Time elapsed (seconds)")
    plt.ylabel("Slots occupied")
    plt.grid(linestyle='dotted', color='black')
    plt.savefig("%s_unequal_intervals.png"%key)
    plt.clf()

def plot_slots_periodic(key):
    timestamps = data[key]['master_data']['tasks']
    num_timestamps = len(timestamps) - 1
    
    worker_ids = list(timestamps[0].keys())
    num_workers = len(worker_ids)
    
    timestamp_list = [0]

    plot_object = {}
    for worker_id in worker_ids:
        plot_object[worker_id] = [0]

    start_time = dt.strptime(timestamps[1]['timestamp'], time_format)
    last_time = start_time
        
    for timestamp in timestamps[1:]:
        timestamp_obj = dt.strptime(timestamp['timestamp'], time_format)
        time_diff = (timestamp_obj - last_time).total_seconds()

        if time_diff < 0.5:
            continue
            
        last_time = timestamp_obj
        timestamp_list.append((timestamp_obj - start_time).total_seconds())
        
        for worker_id in worker_ids:
            plot_object[worker_id].append(timestamp[worker_id])

    plt.style.use('ggplot')
            
    for index in range(num_workers):
        plt.plot(timestamp_list, plot_object[worker_ids[index]], color = color_list[index], label="Worker %s" % worker_ids[index], marker='o')
        plt.legend(loc = 'upper left')
        
    plt.title("Slots occupied Vs Time Elapsed: %s" % keys[key.upper()])
    plt.xlabel("Time elapsed (seconds)")
    plt.ylabel("Slots occupied")
    plt.grid(linestyle='dotted', color='black')
    plt.savefig("%s_equal_intervals.png"%key)
    plt.clf()

plot_slots_periodic('ll')
plot_slots_periodic('r')
plot_slots_periodic('rr')

plot_slots('ll')
plot_slots('r')
plot_slots('rr')

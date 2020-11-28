import json
import numpy as np
from datetime import datetime as dt
import matplotlib.pyplot as plt
from scipy.ndimage.filters import gaussian_filter1d

data = {}
keys = {
    'RR': 'Round Robin',
    'R': 'Random',
    'LL': 'Least Loaded'
}
time_format = "%m/%d/%Y %H:%M:%S.%f"
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

color_list = ['red', 'blue', 'orange', 'green', 'black', 'brown']

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
        ysmoothed = gaussian_filter1d(plot_object[worker_ids[index]], sigma=5)
        plt.plot(timestamp_list, ysmoothed, color = color_list[index], label="Worker %s" % worker_ids[index])
        plt.legend()
        
    plt.title("Slots occupied Plot for: %s" % key.upper())
    plt.xlabel("Timestamp")
    plt.ylabel("Slots occupied")
    plt.savefig("%s.png"%key)
    plt.clf()

plot_slots('ll')
plot_slots('r')
plot_slots('rr')












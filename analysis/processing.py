#!/usr/bin/env python
# coding: utf-8

# In[7]:


import os
import re
import json
from copy import deepcopy


# In[8]:


data = {}
raw_data = {}
keys = ['rr', 'll', 'r']


# In[9]:


worker_file_name_pattern = 'worker_(.*).log'

for key in keys:
    files = os.listdir('./logs/%s/' % key)
    num_workers = len(files) - 1
    
    master_data = None
    worker_data = {}
    for file in files:
        
        file_pointer = open('./logs/%s/%s' % (key, file), 'r')
        file_data = file_pointer.readlines()
        
        if file == 'yacs.log':
            master_data = file_data
            continue
            
        worker_id = re.match('worker_(.*).log', file, re.DOTALL).group(1)
        if not worker_id:
            continue
        
        worker_data[worker_id] = file_data
        
    raw_data[key] = {}
    raw_data[key]['master_data'] = master_data
    raw_data[key]['worker_data'] = worker_data


# In[10]:


worker_task_received_pattern = 'INFO (\d+/\d+/\d+) (\d+:\d+:\d+\.\d+): task recieved: (.*) of job: (.*)'
worker_task_completed_pattern = 'INFO (\d+/\d+/\d+) (\d+:\d+:\d+\.\d+): task completed: (.*) of job: (.*)'

job_received_pattern = 'INFO (\d+/\d+/\d+) (\d+:\d+:\d+\.\d+): scheduling job: (.+) recieved from .*'
job_completed_pattern = 'INFO (\d+/\d+/\d+) (\d+:\d+:\d+\.\d+): job (.+) completed'

master_task_scheduled_pattern = 'INFO (\d+/\d+/\d+) (\d+:\d+:\d+\.\d+): task (.*) from (.*) scheduled on (.*)'
master_task_completed_pattern = 'INFO (\d+/\d+/\d+) (\d+:\d+:\d+\.\d+): completed task (.*)'


# In[11]:


data = {}
for key in keys:
    master_data = raw_data[key]['master_data']
    worker_data = raw_data[key]['worker_data']
    
    # Find Master Data
    jobs_buffer = {}
    jobs = []
    
    worker_ids = list(worker_data.keys())
    initial_timestamp = {}
    for worker_id in worker_ids:
        initial_timestamp[worker_id] = 0
        
    tasks = []
    tasks_buffer = {}
    tasks.append(initial_timestamp)
    
    for line in master_data:
        match_obj = re.match(job_received_pattern, line.strip(), re.DOTALL)
        if match_obj:
            date = match_obj.group(1)
            time = match_obj.group(2)
            job_id = match_obj.group(3)
            
            jobs_buffer[job_id] = date + ' ' + time
            continue
            
        match_obj = re.match(job_completed_pattern, line.strip(), re.DOTALL)
        if match_obj:
            date = match_obj.group(1)
            time = match_obj.group(2)
            job_id = match_obj.group(3)
            if job_id not in jobs_buffer:
                continue
            receive_time = jobs_buffer[job_id]
            
            jobs.append({
                'job_id': job_id,
                'arrival_time': receive_time,
                'end_time': date + ' ' + time
            })
            continue
            
        match_obj = re.match(master_task_scheduled_pattern, line.strip(), re.DOTALL)
        if match_obj:
            date = match_obj.group(1)
            time = match_obj.group(2)
            task_id = match_obj.group(3)
            worker_id = match_obj.group(5)
            
            curr_timestamp = deepcopy(tasks[-1])
            curr_timestamp[worker_id] += 1
            curr_timestamp['timestamp'] = date + ' ' + time
            tasks_buffer[task_id] = worker_id
            tasks.append(curr_timestamp)
            
        match_obj = re.match(master_task_completed_pattern, line.strip(), re.DOTALL)
        if match_obj:
            date = match_obj.group(1)
            time = match_obj.group(2)
            task_id = match_obj.group(3)
            
            worker_id = tasks_buffer[task_id]
            
            curr_timestamp = deepcopy(tasks[-1])
            curr_timestamp[worker_id] -= 1
            curr_timestamp['timestamp'] = date + ' ' + time
            tasks.append(curr_timestamp)
    master_data = {
        'jobs': jobs,
        'tasks': tasks
    }
    
    #Find Worker Data
    
    tasks_buffer = {}
    worker_info = {}
    
    for worker_id in worker_data:
        
        worker_tasks = []
        
        for line in worker_data[worker_id]:
            match_obj = re.match(worker_task_received_pattern, line.strip(), re.DOTALL)
            if match_obj:
                date = match_obj.group(1)
                time = match_obj.group(2)
                task_id = match_obj.group(3)

                tasks_buffer[task_id] = date + ' ' + time
                continue

            match_obj = re.match(worker_task_completed_pattern, line.strip(), re.DOTALL)
            if match_obj:
                date = match_obj.group(1)
                time = match_obj.group(2)
                task_id = match_obj.group(3)
                if task_id not in tasks_buffer:
                    continue
                receive_time = tasks_buffer[task_id]

                worker_tasks.append({
                    'task_id': task_id,
                    'arrival_time': receive_time,
                    'end_time': date + ' ' + time
                })
                continue

        worker_info[worker_id] = worker_tasks
        
    data[key] = {
        'master_data': master_data,
        'worker_data': worker_info
    }


# In[12]:


with open('data.json', 'w') as fp:
    json.dump(data, fp)


# In[ ]:





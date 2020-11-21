import json
import socket
import time
import sys
import random
import numpy as np

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

def send_request(job_request):
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
		s.connect(("localhost", 5000))
		message=json.dumps(job_request)
		#send task
		s.send(message.encode())


if __name__ == '__main__':
	if(len(sys.argv)!=2):
		print("Usage: python requests.py <number_of_requests>")
		exit()

	#get number of requests to be generated
	number_of_requests=int(sys.argv[1])
	arrivals = np.random.exponential(1, size=number_of_requests-1)
	request_number=0
	#send first request
	current_time=last_request_time=time.time() # time 0
	job_request=create_job_request(str(request_number))
	print("interval: ",0,"\n Job request :",job_request)
	send_request(job_request)
	request_number+=1
	while request_number<number_of_requests:
		interval=arrivals[request_number-1]
		while True:
			if(time.time()-last_request_time>=interval):
				break
			time.sleep(0.01)
		job_request=create_job_request(str(request_number))
		print("interval: ",interval,"\n Job request :",job_request)
		send_request(job_request)
		last_request_time=time.time()
		request_number+=1




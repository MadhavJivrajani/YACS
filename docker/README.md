## Containerising YACS

This folder contains Dockerfiles for master(Dockerfile) and workers(worker\_dockerfile).

### Running the containers:
1. `docker build -t test_master .`
2. `docker build -t w1 -f worker_dockerfile .` - repeate this for as many workers needed.
3. `docker run --network host --name master -e sched=RR test_master`
4. `docker run --network host --name w1 -e id=0 w1`

### Todo:
[  ] bash script to automate - including running `gen_requests.py`.  
[  ] push to Dockerhub
[  ] change according to directory reorganization.

# Yet Another Centralized Scheduler (YACS)

`yacs` is a tool built to simulated centralized scheduling polcies in a distributed setting as a project for the UE18CS322 course: Big Data, at PES University.  

## Components:
`yacs` has 3 main components:  
1. `Master`
	- `Listener`
2. `Scheduler`
3. `Worker`

The `Scheduler` supports 3 scheduling policies:
1. Least Loaded (`LL`)
2. Round Robin (`RR`)
3. Random (`R`)

For more info on these, refer docs for the [scheduler](https://yacs.readthedocs.io/en/latest/api.html#module-yacs.component.scheduler).

## Getting started

### Installation from source

Requirements
- Python 3.6.x

```
git clone https://github.com/MadhavJivrajani/YACS.git
cd YACS
pip install .
```

#### Verify installation
```
yacs --help
```

For more info on how to run the master and workers and other related usage info, refer [quickstart](https://yacs.readthedocs.io/en/latest/quickstart.html)

#### If you'd like to build the docs (optional)
```
cd docs
pip install -r requirements.txt
make html
```

The docs should now be available in the `docs/_build` directory.

### Using Docker 

Run the following commands to start YACS using Docker:
```
cd docker
docker build -t master . # build image for master
docker build -t w1 -f worker_dockerfile . # build image for a worker
```

The images can also be pulled from Dockerhub
- Name of master's docker image: [`aditiahuja/yacs_master`](https://hub.docker.com/repository/docker/aditiahuja/yacs_master)
- Name of worker's docker image: [`aditiahuja/yacs_worker`](https://hub.docker.com/repository/docker/aditiahuja/yacs_worker)
- Pull the images by running:
```
docker pull aditiahuja/yacs_master
docker pull aditiahuja/yacs_worker
```      

#### Running YACS
1. Change directory to `yacs/docker` and run `chmod +x start.sh`
2. In the `yacs/docker` directory, run `./start.sh <scheduling policy> <no. of workers>`. For eg. `./start.sh LL 3`.   
3. Enter the ID and port for each worker and then the number of requests.
4. To inspect the logs, run `docker exec -it master bash -c "cat yacs.log"`.      

## Design

For the design of the system, please refer to the [architecture](ARCHITECTURE.md) and the [dev docs](https://yacs.readthedocs.io/en/latest/dev.html)

## Docs
Docs for `yacs` can be found [here](https://yacs.readthedocs.io/en/latest/index.html)

## Future work
- [ ] Add support for handling faults
	- [ ] at `Master`
	- [ ] at `Worker`
- [ ] Partial slots/multiple slot support
- [ ] Support for tasks other than map and reduce
- [ ] Add support unsynchronized clocks among master and workers (Lamport Clocks?)
- [ ] API at `Master` for retrieving status of jobs

## Team members:
1. [Madhav Jivrajani](https://github.com/MadhavJivrajani)
2. [Sparsh Temani](https://github.com/temanisparsh)
3. [Aditi Ahuja](https://github.com/metonymic-smokey)
4. [Ruchika Shashidhara](https://github.com/RuchikaShashidhara)

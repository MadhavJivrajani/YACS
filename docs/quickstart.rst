.. _quickstart:

Quick Start
^^^^^^^^^^^

Installation
============

Git
---

1. Clone the repo using ``git clone https://github.com/MadhavJivrajani/YACS.git``

2. Change the directory ``cd YACS``
3. Install dependencies
4. Setup a virtualenv if python versions are incompatible 

Dependencies and Requirements
.............................

* ``yacs`` runs on Python 3.6.x
* Installation can be done by running ``pip install .``
* For help on running the CLI, run ``yacs --help``

Building Docs
.............

1. Change directory using ``cd docs``
2. Install dependencies using ``pip install -r requirements.txt``
3. Build the docs by running ``make html``
4. The docs should now be available in ``docs/_build``

Usage
.....

* Running the ``Master``:

::

	yacs master --config=path_to_config --sched=scheduling_policy

* Running the ``Worker``:

	* The ``port`` and ``worker_id`` should be provided according to the ``config`` provided in the ``Master``

::

	yacs worker --port=port --id=worker_id

* Submitting ``jobs`` and testing:

::

	python gen_requests.py <no. of requests>

Docker
------

Run the following commands to start YACS using Docker:

::

	cd docker
	docker build -t master . # build image for master
	docker build -t w1 -f worker_dockerfile . # build image for a worker

The images can also be pulled from Dockerhub:

* Name of master's docker image: ``aditiahuja/yacs_master``
* Name of worker's docker image: ``aditiahuja/yacs_worker``
* Pull the images by running:

::

	docker pull aditiahuja/yacs_master
	docker pull aditiahuja/yacs_worker

Running YACS:

1. In the ``yacs/docker`` directory, run ``./start.sh <scheduling policy> <no. of workers>``. For eg. ``./start.sh LL 3``.
2. Enter the ID and port for each worker and then the number of requests.
3. To inspect the logs, run ``docker exec -it master bash -c "cat yacs.log"``. 

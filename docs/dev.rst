.. _dev:

Dev Docs
^^^^^^^^

Design
======

Terminologies
-------------

* ``slots`` are an abstraction for computational resources on each worker node.
* ``job`` encapsulates the high level goal to be accomplished. For ex: a map-reduce operation on a cluster is an example of a ``job``.
* ``task`` signifies components that make up a ``job``. For ex: ``map`` and ``reduce`` operations fall under the category of a ``task``.
* ``client`` is a user or a process that submits a ``job`` to the ``Master``.

Assumptions
-----------

* It is assumed that there can be only two types of tasks:

	1. ``map``
	2. ``reduce``
* Faults are assumed to never occur.
* All tasks utilize *exactly* one ``slot``.
* Clocks are synchronized across nodes in the cluster (can be overcome by using Lamport timestamps).

.. _master:

Master
------

The ``Master`` class implements the logic for the master node of the cluster.

At a high level, the ``Master`` class does the following:

1. Listens for job job requests from clients. It does this on port ``5000``.
2. Keeps track of each worker nodes and listens for updates from them on port ``5001``.
3. Implements a ``Scheduler`` class that is responsible for scheduling tasks on workers depending on the type of scheduling policy set in ``Master``.

A ``config`` file (``json``) is provided to the ``Master`` which holds information about the workers such as number of ``slots`` per worker, ``worker_id`` of the worker and the ``port`` the worker will listen on for task submissions from the ``Master``.

An example ``config`` file is as follows:

.. code-block:: JSON

	{
		"workers": [
			{
				"worker_id": "0",
				"slots": 5,
				"port": 4000
			},
			{
				"worker_id": "1",
				"slots": 7,
				"port": 4001
			},
			{
				"worker_id": "2",
				"slots": 3,
				"port": 4002
			}
		]
	}

Implementation
..............

* For all purposes of inter-thread communication, the python ``Queue`` class is used. This was chosen since ``Queue`` is thread-safe by default.
* For handling multiple concurrent connections:

	* Everytime there is a new incoming connection on a socket, a connection is established to this and the ``Listener`` class is used to maintain this connection in a seperate thread. 
	* In case some time in the future, implementation changes requires the ``Master`` to send acknowledgements to the client. This is enabled by registering an ``Event`` on the thread maintaining the connection. 
	* A mapping from the ``client`` address to the corresponding ``Listener`` object is maintained for ease of implementation of the above mentioned feature.
* To handle job requests comming in from clients and updates coming in from workers, the following is done:

	* Two seperate queues are maintained (one for job requests and the other for updates).
	* For a job type ``Listener``, the queue for job requests is passed to it and similarly the update queue is passed to the update type of ``Listener``.

		* Doing this allows us to share these queues among multiple threads (safely since ``Queue`` is thread safe).
	* Everytime data is recieved in a ``Listener`` thread, this data along with the address (``ip`` and ``port``) of the machine sending it is ``put`` into the queue.
	* By default, the ``get()`` method of the ``Queue`` class *blocks* if the queue is empty.
	
		* A seperate thread is spawned which polls each queue (blocks if empty) and extracts items from the queue.
* Scheduling related actions are broadly divided into categories:

	1. When a new job request arrives at the ``Master``.
	2. When a ``Worker`` sends an update on a task.

For the architecture, please see :ref:`master_arch`.

.. _worker:

Worker
------

The ``Worker`` class implements the logic for a worker node in the cluster.

At a high level, the ``Worker`` class does the following:

1. Listen for task assignments from the ``Master`` on a port specified as command line argument (``PORT``).
2. After simulating task execution, send an update to the ``Master`` signifying that task execution is completed successfuly.

Implementation
..............

* Maintain a queue of completed tasks which can be polled and completion updates can be sent accordingly to the ``Master``.
* Spawn threads to implement the above mentioned functionality

	1. Spawn a thread to listen for incoming tasks from the ``Master``
	2. Spawn a thread to simulate execution of tasks on the ``Worker``
	3. Spawn a thread to poll the completed queue. Items taken out of this queue are then part of the completion update sent to the master.

For the architecture, please see :ref:`worker_arch`.

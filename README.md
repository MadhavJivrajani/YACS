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
- Python 3.8.x

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
add docker stuff

## Design

add diagram

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

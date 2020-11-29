import os
import sys
import argparse

def parse(args) -> None:
	if args.Entity not in ["worker", "master"]:
		print("unrecognised entity name, allowed: worker, master")
		return
	if args.Entity == "master":
		config_path = args.config
		sched_policy = args.sched
		if sched_policy not in ["LL", "RR", "R"]:
			print("unrecognised sched policy, allowed: LL, RR, R")
			return
		os.system("python3 yacs/component/master.py %s %s" % (config_path, sched_policy))
		sys.exit(0)
	else:
		port = args.port
		worker_id = args.id
		os.system("python3 yacs/component/worker.py %s %s" % (port, worker_id))
		sys.exit(0)

def main() -> None:
	yacs_parser = argparse.ArgumentParser(description='CLI for launching yacs entity')

	yacs_parser.add_argument('Entity',
						metavar='entity',
						type=str,
						help='type of entity to launch (master, worker)')
	yacs_parser.add_argument('--config',
						metavar='config',
						type=str,
						default="config.json",
						help='path to config file')
	yacs_parser.add_argument('--sched',
						metavar='sched',
						type=str,
						default="LL",
						help='scheduling policy (LL, RR, R)')
	yacs_parser.add_argument('--port',
						metavar='port',
						type=int,
						help='port for worker to run on according to config')
	yacs_parser.add_argument('--id',
						metavar='workerID',
						type=str,
						help='id for the worker according to the config')
	

	args = yacs_parser.parse_args()

	parse(args)

if __name__ == "__main__":
	main()

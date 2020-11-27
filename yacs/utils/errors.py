__all__ = ['ThreadTerminate', 'handle_thread_terminate']

class ThreadTerminate(Exception):
	"""Wrapper exception for when a thread terminates"""
	pass

def handle_thread_terminate(signum, frame):
	"""Handler function for graceful termination"""
	raise ThreadTerminate

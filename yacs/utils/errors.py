__all__ = ['ThreadTerminate', 'handle_thread_terminate']

class ThreadTerminate(Exception):
	pass

def handle_thread_terminate(signum, frame):
	raise ThreadTerminate

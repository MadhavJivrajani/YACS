import logging
from logging import Formatter, getLogger, StreamHandler

__all__ = ['CustomFormatter']

class Color:
	def __init__(self) -> None:
		self.colors = {
			'black': 30,
			'red': 31,
			'green': 32,
			'yellow': 33,
			'blue': 34,
			'magenta': 35,
			'cyan': 36,
			'white': 37,
			'bgred': 41,
			'bggreen': 42
		}

		self.prefix = '\033[1;'
		self.suffix = '\033[0m'

	def colorize(self, text, color=None):
		if color not in self.colors:
			color = 'white'

		clr = self.colors[color]
		return (self.prefix+'%dm%s'+self.suffix) % (clr, text)

class CustomFormatter(Formatter):
	def __init__(self) -> None:
		self.colorize = Color().colorize
		self.level_format = "%(levelname)s"
		self.rem_format = " %(asctime)s.%(msecs)03d: %(message)s"

	def format(self, record):
		mapping = {
			'INFO': 'cyan',
			'WARNING': 'yellow',
			'ERROR': 'red',
			'CRITICAL': 'bgred',
			'SUCCESS_TASK': 'green',
			'SUCCESS_JOB': 'bggreen'
		}

		clr = self.colorize(self.level_format, mapping.get(record.levelname, 'white')) \
			+ self.rem_format
		formatter = logging.Formatter(clr, datefmt='%m/%d/%Y %H:%M:%S')

		return formatter.format(record)

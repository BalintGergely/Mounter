

import concurrent.futures
from mounter.workspace import *
from mounter.operation.completion import *
from mounter.operation.bridge import *

A = TypeVar("A")

class Parallel(Module):
	def __init__(self, context):
		super().__init__(context)
		self.ws.add(Bridge)
	
	def run(self):
		self.__threadPool = concurrent.futures.ThreadPoolExecutor()
		try:
			self._downstream()
		finally:
			self.__threadPool.shutdown(cancel_futures = True)
	
	def callInBackground(self, target : Callable[[],A], *args, **kwargs) -> Awaitable[A]:
		target = functools.partial(target, *args, **kwargs)

		bridge = self.ws[Bridge]

		if bridge.isShutdown():
			return Instant(Exception("Bridge is shut down"))

		f = self.__threadPool.submit(target)
		return bridge.bridgeConcurrent(f)


import asyncio
from mounter.workspace import *
from mounter.operation.util import Guardian
from typing import Awaitable

class AsyncioLoop(Module):
	"""
	This module hosts an asyncio event loop for the duration of its existence.
	"""
	def __init__(self, context):
		super().__init__(context)
		self.ws.add(Guardian)
		self.__runner = asyncio.Runner()
		self.__scheduledOrRunning = False
		self.__mustComplete : List[asyncio.Future] = []
	
	def run(self):
		with self.__runner:
			self._downstream()
	
	@property
	def loop(self) -> asyncio.AbstractEventLoop:
		return self.__runner.get_loop()

	def completeNow(self,f : Awaitable):
		"""
		Runs the event loop until the specified awaitable is complete.
		"""
		return self.loop.run_until_complete(f)
	
	def completeLater(self,f : Awaitable):
		"""
		Enqueue the specified awaitable to be completed later.
		"""
		self.__mustComplete.append(asyncio.ensure_future(f,loop = self.loop))
		if not self.__scheduledOrRunning:
			self.ws.append(self.__runUntilAllFuturesDone)
			self.__scheduledOrRunning = True

	def __runUntilAllFuturesDone(self):
		while len(self.__mustComplete) != 0:
			self.loop.run_until_complete(asyncio.gather(*self.__mustComplete, return_exceptions = True))
			self.__mustComplete = [k for k in self.__mustComplete if not k.done()]
		self.__scheduledOrRunning = False	

def manifest() -> Type[AsyncioLoop]:
	return AsyncioLoop
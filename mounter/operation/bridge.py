
import asyncio
import signal
import threading
import concurrent.futures as cf
from contextlib import contextmanager
from mounter.workspace import *
from mounter.operation.completion import *
from mounter.operation.loop import AsyncioLoop

A = TypeVar("A")

class BridgeShutdownException(Exception):
	"""
	Raised from ALL bridge tasks when the bridge shuts down.
	"""
	def __init__(self, *args):
		super().__init__(*args)

class BridgeTask(CompletionFuture):
	def __eq__(self, value):
		return self is value
	
	def __hash__(self):
		return id(self)

	def _completeFromInternalFuture(self, future : asyncio.Future | cf.Future):
		if self.done():
			return
		if future.cancelled():
			self._setException(CancelledException())
		else:
			exc = future.exception()
			if exc is None:
				self._setResult(future.result())
			else:
				self._setException(exc)
	
	def _completeWithNone(self):
		if not self.done():
			self._setResult(None)

	def _completeFromFuture(self, source : Future):
		if not self.done():
			self._copyFrom(source)

class Bridge(Module):
	def __init__(self, context) -> None:
		super().__init__(context)
		self.ws.add(AsyncioLoop)
		self.__isShutdown = False
		self.__bridgeFuture : asyncio.Future = None
		self.__tasks : List[BridgeTask] = [] # INCLUDES daemon tasks
		self.__activeTaskCount : int = 0     # NOT including daemon tasks
		self.__closeHook = CompletableFuture()
		self.__closeHook.thenCall(self.__interruptAllTasks)

	def run(self):
		self.__loop = self.ws[AsyncioLoop].loop
		with self.__registerSigint():
			try:
				self._downstream()
			finally:
				self.__isShutdown = True
				self.__checkShutdown()
	
	def __checkShutdown(self):
		rs = self.__isShutdown
		if rs and not self.__closeHook.done():
			self.__closeHook.setException(BridgeShutdownException())
		return rs
	
	def __newTask(self, daemonTask : bool = False) -> BridgeTask:
		bridge = BridgeTask()

		self.__tasks.append(bridge)

		if not daemonTask:
			self.__activeTaskCount += 1
			bridge.then(self.__onTaskDone)
			self.__scheduleRunUntilTasksDone()

		return bridge
	
	def __interruptAllTasks(self):
		for t in self.__tasks:
			t._completeFromFuture(self.__closeHook)
	
	def __onTaskDone(self, _ : BridgeTask):
		self.__activeTaskCount -= 1

		if self.__activeTaskCount == 0:
			# We delay stopping for one more pass as it is possible
			# that a new task is scheduled right after the callback we are a part of.
			self.__loop.call_soon(self.__stopLoop)
		
		if self.__activeTaskCount * 2 < len(self.__tasks):
			self.__pruneTasks()
	
	def __stopLoop(self):
		if self.__activeTaskCount == 0:
			self.__bridgeFuture.set_result(None)
			self.__bridgeFuture = None
	
	def __pruneTasks(self):
		self.__tasks = [t for t in self.__tasks if not t.done()]
	
	def __scheduleRunUntilTasksDone(self):
		if self.__bridgeFuture is None:
			self.__bridgeFuture = self.__loop.create_future()
			self.ws[AsyncioLoop].completeLater(self.__bridgeFuture)
	
	@DelayerMethod
	def delay(self, proc : Callable[[Self], None]):
		if self.__checkShutdown():
			proc()
			return
		
		bridge = self.__newTask()
		self.__loop.call_soon(bridge._completeWithNone)
		bridge.thenCall(proc)
	
	def bridgeTask(self, task : Awaitable[A]) -> CompletionFuture[A]:
		"""
		Bridge a completion awaitable. The resulting completion will complete when
		the argument task completes or when the bridge is shut down. Whichever happens sooner.

		The completion will keep the bridge open.
		"""
		if self.__checkShutdown():
			if isinstance(task,Coroutine):
				task.close()
			return self.__closeHook.minimal()
		
		task = Task(task)
		if task.done():
			return task

		bridge = self.__newTask()
		task.then(bridge._completeFromFuture)

		return bridge
	
	def bridgeAsync(self, task : Awaitable[A]) -> CompletionFuture[A]:
		"""
		Bridge an asyncio awaitable.
		"""
		if self.__checkShutdown():
			if asyncio.iscoroutine(task):
				task.close()
			elif isinstance(task, asyncio.Future):
				task.cancel()
			return self.__closeHook.minimal()

		task = self.__loop.create_task(task)

		bridge = self.__newTask()
		task.add_done_callback(bridge._completeFromInternalFuture)
		bridge.thenCall(task.cancel)

		return bridge
	
	def timeout(self, seconds : float) -> CompletionFuture[None]:
		"""
		Returns a completion that will complete with the specific timeout or when
		this bridge shuts down. Whichever happens sooner.

		**Note** that unlike most bridge tasks, the timeout
		completion does not keep the bridge open. It can complete as late
		as the bridge **Module** cleanup!
		(It will complete then the latest, as all completions must complete.)
		"""
		if self.__checkShutdown():
			return self.__closeHook.minimal()
		
		bridge = self.__newTask(daemonTask=True)
		h = self.__loop.call_later(seconds, bridge._completeWithNone)
		bridge.thenCall(h.cancel)

		return bridge

	def onShutdown(self) -> CompletableFuture[None]:
		"""
		Returns a completion that will complete when this bridge shuts down.

		**Note** that this can complete as late as the bridge **Module** cleanup!
		(It will complete then the latest, as all completions must complete.)
		"""
		return self.__closeHook.minimal()

	def bridgeConcurrent(self, task : cf.Future[A]) -> CompletionFuture[A]:
		"""
		Bridge a concurrent awaitable.
		"""
		if self.__checkShutdown():
			task.cancel()
			return self.__closeHook.minimal()
		
		bridge = self.__newTask()

		task.add_done_callback(
			functools.partial(self.__loop.call_soon_threadsafe,bridge._completeFromInternalFuture)
		)
		
		bridge.thenCall(task.cancel)

		return bridge
		
	async def bridgeIter(self, itr : AsyncIterator[A]) -> AsyncGenerator[A]:
		"""
		Bridge an async iterator.
		"""
		iterCoro = itr.asend(None)
		while True:
			try:
				coroResult = await self.bridgeAsync(iterCoro)
			except StopAsyncIteration:
				return
			else:
				try:
					inject = yield coroResult
				except GeneratorExit:
					iterCoro = itr.aclose()
				except BaseException as exc:
					iterCoro = itr.athrow(exc)
				else:
					iterCoro = itr.asend(inject)
	
	def isShutdown(self):
		return self.__isShutdown

	def shutdown(self):
		self.__isShutdown = True
		try:
			self.__loop.call_soon_threadsafe(self.__checkShutdown)
		except RuntimeError:
			pass
	
	@contextmanager
	def __registerSigint(self):
		if threading.current_thread() is not threading.main_thread():
			yield None
			return

		h = signal.signal(signal.SIGINT, self.__onSigint)
		try:
			yield None
		finally:
			signal.signal(signal.SIGINT, h)

	def __onSigint(self, *_):
		self.shutdown()

class RedLight(Module,Delayer,Awaitable[None]):
	def __init__(self, context):
		super().__init__(context)
		self.ws.add(Bridge)
		self.__redLight : None | Completion = None
	
	def __refreshRedLight(self):
		if self.__redLight is None or self.__redLight.done():
			bridge = self.ws[Bridge]
			if bridge.isShutdown():
				return
			self.__redLight = Completion.fromDelayer(bridge.delay)
	
	def then(self, proc):
		self.thenCall(proc,self)
	
	def thenCall(self, proc, *args, **kwargs):
		self.__refreshRedLight()
		self.__redLight.thenCall(proc, *args, **kwargs)

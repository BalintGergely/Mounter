
import asyncio
import concurrent.futures
import subprocess
import functools
import concurrent
import signal
from typing import TypeVarTuple, Tuple, Awaitable, Callable, Type
from mounter.workspace import Module
from mounter.operation.completion import *

A = TypeVar("A")
P = ParamSpec("P")
T = TypeVarTuple("T")

class BaseOpTask(CompletionFuture[A]):
	def __eq__(self, value):
		return self is value
	
	def __hash__(self):
		return id(self)

	def tryCancel(self):
		pass

class RedLightTask(BaseOpTask):
	def __new__(cls, loop : asyncio.AbstractEventLoop):
		self = super().__new__(cls)
		loop.call_soon(self.__callback)
		return self
	
	@override
	def tryCancel(self):
		if not self.done():
			self._setException(CancelledException())
	
	def __callback(self):
		if not self.done():
			self._setResult(None)

class AsyncioCommandTask(BaseOpTask[Tuple[int,bytes,bytes]]):
	__task : asyncio.Task
	__process : asyncio.subprocess.Process

	def __new__(cls, loop : asyncio.AbstractEventLoop, commandStr : str, input : bytes):
		self = super().__new__(cls)
		self.__task = loop.create_task(self.__runCommandOnAsyncio(commandStr, input))
		self.__task.add_done_callback(self.__taskDoneCallback)
		self.__process = None
		return self
	
	def __taskDoneCallback(self,fut):
		self._copyFrom(AsyncTask(fut))
		
	@override
	def tryCancel(self):
		self.__task.cancel()
	
	async def __runCommandOnAsyncio(self, commandStr : str, input : bytes):
		self.__process = await asyncio.create_subprocess_shell(commandStr,
					stdout=asyncio.subprocess.PIPE,
					stderr=asyncio.subprocess.PIPE,
					stdin=asyncio.subprocess.PIPE)
		
		try:
			(stdout,stderr) = await self.__process.communicate(input = input)
		except asyncio.CancelledError:
			await self.__process.wait()
			raise
		
		return (self.__process.returncode, stdout, stderr)

class ThreadPoolBackgroundTask(BaseOpTask[A]):
	__delay : Delayer
	def __new__(cls, command : Callable[[],A], pool : concurrent.futures.ThreadPoolExecutor, delay : Delayer):
		self = super().__new__(cls)
		self.__delay = delay
		self.__delay.thenCall(self.__createTask, command, pool)
		return self
	
	def __createTask(self, command : Callable[[],A], pool : concurrent.futures.ThreadPoolExecutor):
		if not self.done():
			self.__task = pool.submit(command)
			self.__task.add_done_callback(self.__taskDoneCallbackUnsafe)

	def __taskDoneCallbackUnsafe(self,fut):
		self.__delay.thenCall(self.__taskDoneCallback,fut)

	def __taskDoneCallback(self,fut : concurrent.futures.Future):
		if fut.cancelled():
			self._setException(CancelledException())
		else:
			ex = fut.exception()
			if ex is None:
				self._setResult(fut.result())
			else:
				self._setException(ex)
		
	@override
	def tryCancel(self):
		if self.done():
			return
		if self.__task is None:
			self._setException(CancelledException())
		else:
			self.__task.cancel()

class AsyncOps(Module):
	"""
	Module for async execution.

	DO NOT use asyncio! It is NOT COMPATIBLE with AsyncOps.
	"""
	def __init__(self, context) -> None:
		super().__init__(context)
		self.__runner = asyncio.Runner()
		self.__threadPool = concurrent.futures.ThreadPoolExecutor()
		self.__runnerDelayer = None
		self.__interruptState = CompletableFuture()
		self.__mustComplete : List[Future] = []
		self.__seqMode = False
		self.__attached : List[BaseOpTask] = None
		self.__currentRedLight = None
		self.__rawInterrupted : bool = False
	
	def disableAsync(self):
		self.__seqMode = True
	
	def __unsafeInterrupt(self, *_):
		self.__rawInterrupted = True
		self.__getLoop().call_soon_threadsafe(self.__safeInterrupt)
	
	def __safeInterrupt(self, *_):
		self.__getLoop().call_soon_threadsafe(self.__setInterruptState,KeyboardInterrupt())

	def run(self):
		h = signal.signal(signal.SIGINT, self.__unsafeInterrupt)
		try:
			with self.__runner:
				try:
					self.__runnerDelayer = AsyncDelayer(self.__runner.get_loop())
					self._downstream()
				finally:
					try:
						self.__setInterruptState(Exception("You shouldn't see this."))
					finally:
						self.__threadPool.shutdown(wait = True)
				for c in self.__mustComplete:
					c.result()
		finally:
			signal.signal(signal.SIGINT, h)

	def __getLoop(self):
		return self.__runner.get_loop()
	
	def completeLater(self,fut : Future):
		"""
		Adds the future to a list of futures that are validated when this AsyncOps is closed.
		If the future fails or is incomplete, an exception will be raised.

		This is the main way to catch exceptions.
		"""
		assert isinstance(fut, Future)
		self.__mustComplete.append(fut)
	
	def __setInterruptState(self, x : BaseException):
		try:
			if not self.__interruptState.done():
				self.__interruptState.setException(wrapException(x))
		finally:
			if self.__attached is not None:
				for t in list(self.__attached):
					t.tryCancel()
	
	def __onAttachedTaskDone(self, fut : BaseOpTask):
		self.__attached.remove(fut)
		if len(self.__attached) == 0:
			loop = self.__getLoop()
			if loop.is_running():
				loop.stop()
			return False
		else:
			return True

	def __runUntilAllAttachedDone(self):
		while len(self.__attached) != 0:
			self.__getLoop().run_forever()
		self.__attached = None
	
	def __attach(self, task : BaseOpTask):
		"""
		Attach a task to the state of the AsnyncOps.
		Returns an attached task, which will either fail or complete.
		"""

		if task.done():
			return task.minimal()

		if self.__attached is None:
			self.__attached = [task]
			self.ws.append(self.__runUntilAllAttachedDone)
		else:
			self.__attached.append(task)
		
		task.then(self.__onAttachedTaskDone)
		
		return task.minimal()

	def redLight(self) -> Awaitable:
		"""
		Yields async execution.
		
		It is recommended to wait on this before the first time an operation
		runs heavy computation, but only after dependent operations have been dispatched.

		This essentially suppresses fail-fast behaviour for awaiters, unless running in sequential mode.
		"""
		if self.__seqMode:
			return INSTANT
		
		# It is best if the red lights are bunched together.
		# They commonly preceede subprocess calls, which we would like to
		# start as soon as possible, preferrably sooner than any task
		# on the pooled threads, which slow down everything due to GIL.

		# For the purposes of the progress bar, all progress units
		# are supposed to be registered before the first red light await returns.
		
		if self.__currentRedLight is None or self.__currentRedLight.done():
			self.__currentRedLight = self.__attach(RedLightTask(self.__getLoop()))

		return self.__currentRedLight
	
	async def runCommand(self, command, input = bytes(), *, progressUnit = None) -> Tuple[int, bytes, bytes]:
		"""
		Runs the specified command. If given, the progressUnit is
		set to running before the command is actually started.

		Returns the exit code, the output, and error output of the command.
		"""
		if self.__interruptState.failed():
			return await self.__interruptState.minimal()

		commandStr = subprocess.list2cmdline(str(c) for c in command)

		if progressUnit is not None:
			progressUnit.setRunning()
		
		task = AsyncioCommandTask(self.__getLoop(), commandStr, input)

		if self.__seqMode:
			self.__getLoop().run_until_complete(task.toAsyncioFuture(self.__getLoop()))

		result = await self.__attach(task)

		if self.__rawInterrupted:
			raise InterruptedException()
		
		return result
	
	def callInBackground(self, command : Callable[[],A]) -> CompletionFuture[A]:
		"""
		Submits the specified callable to be executed on a background (Python) thread asynchronously.
		
		A CompletionFuture is returned representing the future result of the call.
		"""
		if self.__interruptState.failed():
			return self.__interruptState.minimal()

		if self.__seqMode:
			cp = CompletableFuture()
			cp.callAndSetResult(command)
			return cp

		return self.__attach(ThreadPoolBackgroundTask(command, self.__threadPool, self.__runnerDelayer))

def manifest() -> Type[AsyncOps]:
	return AsyncOps

def once(fun : Callable[P,A]) -> Callable[P,A]:
	"""
	The decorated method is run only once per unique set of arguments.
	This does NOT support default arguments.
	"""
	attrName = f"op{id(fun)}"
	@functools.wraps(fun)
	def wrapper(self, *args, **kwargs):
		key = (args,frozenset(kwargs.items()))
		cache = getattr(self, attrName, None)
		if cache is None:
			cache = dict()
			setattr(self, attrName, cache)
		if key not in cache:
			cache[key] = fun(self, *args)
		return cache[key]
	return wrapper

def task(coro : Callable[P,Awaitable[A]]) -> Callable[P,CompletionFuture[A]]:
	"""
	The decorated coroutine is wrapped in a completion Task.
	The task is explicitly not fail-fast.
	"""
	@functools.wraps(coro)
	def wrapper(*args,**kwargs):
		return Task(coro(*args,**kwargs))
	return wrapper

def operation(fun : Callable[P,Awaitable[A]]) -> Callable[P,CompletionFuture[A]]:
	"""
	A composition of the once and task decorators, with the addition of fail-fast behaviour.
	The decorated async method is ran asynchronously once per unique set of arguments.
	
	If it fails before the decorated method returns to the caller, the exception is raised immediately.
	This behaviour can be modified by setting the "failFast" keyword argument to false.
	"""
	fun = once(task(fun))
	@functools.wraps(fun)
	def wrapper(*args,failFast : bool = True,**kwargs):
		k = fun(*args,**kwargs)
		if failFast and k.failed():
			raise k.exception()
		return k
	return wrapper

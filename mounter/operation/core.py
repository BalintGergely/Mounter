
import asyncio
import concurrent.futures
import subprocess
import functools
import concurrent
from typing import TypeVarTuple, Tuple, Awaitable, Callable, Type
from mounter.workspace import Module
from mounter.operation.completion import *

A = TypeVar("A")
P = ParamSpec("P")
T = TypeVarTuple("T")

class AsyncOps(Module):
	"""
	Module for async execution.

	DO NOT use asyncio! It is NOT COMPATIBLE with AsyncOps.
	"""
	def __init__(self, context) -> None:
		super().__init__(context)
		self.__runner = asyncio.Runner()
		self.__runnerDelayer = None
		self.__interruptState = CompletableFuture()
		self.__mustComplete : List[Future] = []
		self.__seqMode = False
		self.__attached : List[Completion] | None = None
		self.__currentRedLight = None
		self.__threadPool = concurrent.futures.ThreadPoolExecutor()
	
	def disableAsync(self):
		self.__seqMode = True
	
	def run(self):
		with self.__runner:
			try:
				self.__runnerDelayer = AsyncDelayer(self.__runner.get_loop())
				self._downstream()
			finally:
				try:
					self.__setInterruptState(Exception("You shouldn't see this."))
				finally:
					self.__threadPool.shutdown(wait = True, cancel_futures = True)
			for c in self.__mustComplete:
				c.result()

	def __getLoop(self):
		return self.__runner.get_loop()
	
	def __runLoopUntilDone(self, task : Completion):
		if not task.done():
			loop = self.__getLoop()
			task.then(lambda x:loop.stop())		
			try:
				loop.run_forever()
			except BaseException as x:
				self.__setInterruptState(x)
				raise
	
	def completeNow(self,coro : Awaitable[A]) -> A:
		"""
		Block until the specified Awaitable is done. Return it's result or raise it's exception.
		"""
		task = Task(coro)
		self.__runLoopUntilDone(task)
		return task.result()
	
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
			self.__threadPool.shutdown(wait = False, cancel_futures = True)
		finally:
			if not self.__interruptState.done():
				self.__interruptState.setException(wrapException(x))

	def __drainAttached(self):
		exceptions = []
		while len(self.__attached) != 0:
			task = self.__attached[0]
			if task.done():
				self.__attached.pop(0)
			else:
				try:
					self.__runLoopUntilDone(task)
				except BaseException as x:
					self.__setInterruptState(x)
					exceptions.append(x)
		self.__attached = None
		if len(exceptions) != 0:
			for x in exceptions:
				absorbException(x)
			raise ExceptionGroup("",exceptions = exceptions)
	
	def __attach(self, task : Completion):
		"""
		Attaches a task to the state of the AsnyncOps.
		Returns an attached task, which will either fail or complete.
		"""

		task = Gather(task, self.__interruptState, policy = cancelPolicy)

		if self.__attached is None:
			self.__attached = [task]
			self.ws.append(self.__drainAttached)
		else:
			self.__attached.append(task)
		
		return task

	def __ensureNotInterrupted(self):
		if self.__interruptState.failed():
			raise self.__interruptState.exception()
	
	def redLight(self) -> Awaitable:
		"""
		Yields async execution.
		
		It is recommended to wait on this before the first time an operation
		runs heavy computation, but only after dependent operations have been dispatched.

		Note that red light may raise an exception if an interrupt has been issued.
		"""
		if self.__seqMode:
			return Instant()
		
		# It is best if the red lights are bunched together.
		# They commonly preceede subprocess calls, which we would like to
		# start as soon as possible, preferrably sooner than any task
		# on the pooled threads, which slow down everything due to GIL.
		
		if self.__currentRedLight is None or self.__currentRedLight.done():
			self.__currentRedLight = self.__attach(AsyncCompletion(self.__getLoop()))

		return self.__currentRedLight
	
	async def __asyncioRunCommand(self, commandStr : str, input : bytes):
		proc = await asyncio.create_subprocess_shell(commandStr,
					stdout=asyncio.subprocess.PIPE,
					stderr=asyncio.subprocess.PIPE,
					stdin=asyncio.subprocess.PIPE)
		
		(stdout, stderr) = await proc.communicate(input = input)

		return (proc.returncode, stdout, stderr)
		
	async def runCommand(self, command, input = bytes(), *, progressUnit = None) -> Tuple[int, bytes, bytes]:
		"""
		Runs the specified command. If given, the progressUnit is
		set to running before the command is actually started.

		Returns the exit code, the output, and error output of the command.
		"""
		self.__ensureNotInterrupted()

		commandStr = subprocess.list2cmdline(str(c) for c in command)

		if progressUnit is not None:
			progressUnit.setRunning()
		
		asyf = self.__getLoop().create_task(self.__asyncioRunCommand(commandStr, input = input))

		task = AsyncTask(asyf, loop = self.__getLoop())

		if self.__seqMode:
			self.__runLoopUntilDone(task)

		result = await self.__attach(task)

		return result
	
	def callInBackground(self, command : Callable[[],A]) -> CompletionFuture[A]:
		"""
		Submits the specified callable to be executed on a background (Python) thread asynchronously.
		
		A CompletionFuture is returned representing the future result of the call.
		"""
		self.__ensureNotInterrupted()

		if self.__seqMode:
			cp = CompletableFuture()
			cp.callAndSetResult(command)
			return cp

		unsafeCompletable = CompletableFuture()
		safeCompletable = unsafeCompletable.withDelay(self.__runnerDelayer)
		submitCommand = functools.partial(unsafeCompletable.callAndSetResult,command)

		# Global interpreter lock has funny behaviour where the current thread
		# is immediately halted when a new thread is created.
		# Therefore it is best if we delay submission using our async loop first.

		def doSubmit():
			if not self.__interruptState.failed():
				try:
					self.__threadPool.submit(submitCommand)
				except BaseException as ex:
					self.__setInterruptState(ex)
					absorbException(ex)
		
		self.__getLoop().call_soon(doSubmit)

		return self.__attach(safeCompletable)

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
	Watch out for hanging tasks!
	"""
	@functools.wraps(coro)
	def wrapper(*args,**kwargs):
		return Task(coro(*args,**kwargs))
	return wrapper

def op(arg : Callable[P,Awaitable[A]]) -> Callable[P,CompletionFuture[A]]:
	"""
	A composition of the once and task decorators.
	The decorated async method is ran asynchronously once per unique set of arguments.
	"""
	return once(task(arg))

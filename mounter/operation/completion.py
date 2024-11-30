
from typing import *
import asyncio
import functools

A = TypeVar('A')
B = TypeVar('B')
C = TypeVar('C')
D = TypeVar('D')
T = TypeVarTuple('T')

def absorbException(exc : BaseException):
	"""
	If the argument reports SystemExit or KeyboardInterrupt, the corresponding exception is raised.
	"""
	if isinstance(exc,BaseExceptionGroup):
		for x in exc.exceptions:
			absorbException(x)
	if isinstance(exc, SystemExit | KeyboardInterrupt):
		raise exc

def isInterrupt(exc : BaseException):
	"""
	Tests whether the exception is being caused by an event that occurred
	out of scope.
	"""
	if isinstance(exc,BaseExceptionGroup):
		for x in exc.exceptions:
			if not isInterrupt(x):
				return False
	return not isinstance(exc, SystemExit | KeyboardInterrupt | CancelledException)

class BaseCompletionException(Exception):
	pass

class CancelledException(BaseCompletionException):
	pass

class Future(Generic[A]):
	"""
	A placeholder for the outcome of an asynchronous operation.
	Waiting for the outcome is not always supported.
	"""
	__result : Tuple[bool,A | BaseException] | None
	def __new__(cls) -> Self:
		self = super().__new__(cls)
		self.__result = None
		return self
	
	def _setResult(self,resultValue):
		self.__result = (False,resultValue)
	
	def _setException(self,exceptionValue):
		self.__result = (True,exceptionValue)
	
	def _copyFrom(self, source : 'Future[A]'):
		assert source.done()
		self.__result = source.__result

	def done(self):
		"""
		Returns True if this future has the result ready. False otherwise.
		"""
		return self.__result is not None
		
	def result(self) -> A:
		"""
		Returns the result of the Future, or raises the exception.
		"""
		(doRaise,what) = self.__result
		if doRaise:
			raise what
		else:
			return what
	
	def exception(self):
		"""
		Returns the exception of the Future, or None if completed normally.
		"""
		(doRaise,what) = self.__result
		if doRaise:
			return what
		else:
			return None
	
	def copyToAsyncioFuture(self, future : asyncio.Future):
		"""
		Assigns the result of this Future to the specified asyncio future.
		"""
		(doRaise,what) = self.__result
		if doRaise:
			future.set_exception(what)
		else:
			future.set_result(what)

class Delayer():
	"""
	A delayer to which callbacks can be added.
	Callbacks are invoked later at an unspecified time, with the Delayer
	as their only argument.

	This is the base class of awaitables in this module.
	"""
	def then(self,proc : Callable[[Self], None]):
		"""
		Invokes the procedure at some unspecified time in the future.
		The procedure's only argument will be this Delayer.

		May invoke immediately.
		"""
		raise Exception("Not implemented!")
	
	def thenCall(self,proc : Callable[[*T], None],*args : *T,**kwargs):
		"""
		Alternative to 'then' where proc receives the specified arguments.
		"""
		self.then(lambda _: proc(*args,**kwargs))
	
	def __await__(self):
		yield self
	
	def __iter__(self):
		return (yield from self.__await__())

class LoopDelayer(Delayer):
	"""
	A LoopDelayer hosts a mini event loop to help mitigate excessive
	stack consumption.
	
	The first task that is added runs immediately. While it is running,
	further tasks are added to a queue.

	When the first task completes, a new task is taken from the queue until
	all tasks are processed.

	The 'then' method returns immediately when recursively invoked.
	Otherwise it processes tasks until the queue is empty.
	"""
	__queue : List[Callable[[Self],None]]
	__loopRunning : bool
	def __new__(cls) -> Self:
		self = super().__new__(cls)
		self.__queue = []
		self.__loopRunning = False
		return self
	
	def then(self, proc: Callable[[Self], Any]):
		self.__queue.append(proc)
		
		if self.__loopRunning:
			return
		
		self.__loopRunning = True
		try:
			exceptions = []
			while len(self.__queue) != 0:
				proc = self.__queue.pop()
				try:
					proc(self)
				except BaseException as exc:
					exceptions.append(exc)
		finally:
			self.__loopRunning = False
		
		if len(exceptions) != 0:
			for e in exceptions:
				absorbException(e)
			raise ExceptionGroup("",exceptions = exceptions)

class QueueDelayer(Delayer):
	__queue : List[Callable[[Self],None]]
	def __new__(cls) -> Self:
		self = super().__new__(cls)
		self.__queue = []
		return self
	
	def then(self, proc: Callable[..., Any]):
		self.__queue.append(proc)
	
	def waiting(self):
		return len(self.__queue)
	
	def run(self, count : int | None = None):
		tasksRun = 0
		while tasksRun != count and len(self.__queue) != 0:
			proc = self.__queue.pop(0)
			proc(self)
			tasksRun += 1
		return tasksRun

class DelegatedDelayer(Delayer):
	__delegate : Callable[[Callable], None]
	def __new__(cls, delegate : Callable[[Callable], None]) -> Self:
		self = super().__new__(cls)
		self.__delegate = delegate
		return self
	
	def then(self,proc : Callable):
		self.__delegate(functools.partial(proc,self))

class Completion(Delayer):
	"""
	A queue of callbacks that will be run when a certain event occurs.
	If the event has already occurred, further callbacks are run immediately on submission.
	This is Awaitable.
	"""
	__queue : List[Callable] | Tuple
	def __new__(cls) -> Self:
		self = super().__new__(cls)
		self.__queue = []
		return self
	
	def _complete(self):
		exceptions = []
		queue = self.__queue
		self.__queue = None
		for a in queue:
			try:
				a(self)
			except BaseException as exc:
				exceptions.append(exc)
		if len(exceptions) != 0:
			for k in exceptions:
				absorbException(k)
			raise ExceptionGroup("",exceptions = exceptions)
	
	def done(self):
		return self.__queue is None
	
	@override
	def then(self,proc : Callable):
		if self.__queue != None:
			self.__queue.append(proc)
		else:
			proc(self)
	
	def __await__(self):
		if self.__queue != None:
			yield self

class Completed(Completion):
	"""
	A Completion that is completed with None as it's value.
	"""
	__instance : 'Completed | None' = None
	def __new__(cls) -> Self:
		if cls is not Completed:
			self = super().__new__(cls)
			self._complete()
			return self
		if Completed.__instance is None:
			Completed.__instance = super().__new__(cls)
			Completed.__instance._complete()
		return Completed.__instance

class CompletionFuture(Future[A],Completion):
	"""
	Combination of Future and Completion. Awaiting additionally returns the result, or raises the exception.
	"""
	@override
	def _setResult(self, resultValue):
		super()._setResult(resultValue)
		super()._complete()
	
	@override
	def _setException(self, exceptionValue):
		super()._setException(exceptionValue)
		super()._complete()
	
	@override
	def _copyFrom(self, source : 'Future[A]'):
		super()._copyFrom(source)
		super()._complete()
	
	@override
	def _complete(self):
		raise Exception("_complete may not be called directly on CompletionFuture.")

	@override
	def __await__(self):
		yield from super().__await__()
		return self.result()
		
	def withDelay(self,delayer : Delayer) -> 'CompletionFuture[A]':
		"""
		Returns a new completion, that is completed after this completion is completed
		with the specified additional delay. The delay is applied even if this completion
		has already completed. The result of the new completion is the same as this.
		"""
		newCompletion = CompletionFuture()

		self.then(functools.partial(delayer.thenCall,newCompletion._copyFrom))

		return newCompletion

	def toAsyncioFuture(self, loop : asyncio.AbstractEventLoop | None = None):
		"""
		Returns an asyncio future that is completed when this completion is completed.
		"""
		if loop is None:
			loop = asyncio.get_event_loop()
		future = loop.create_future()
		self.thenCall(functools.partial(self.copyToAsyncioFuture,future))
		return future

class CompletableFuture(CompletionFuture):
	def __new__(cls) -> Self:
		return super().__new__(cls)
	
	def setResult(self, resultValue):
		return self._setResult(resultValue)
	
	def setException(self, exceptionValue):
		return self._setException(exceptionValue)
	
	def callAndSetResult(self, proc : Callable[[*T],A], *args : *T, **kwargs):
		try:
			result = proc(*args,**kwargs)
		except BaseException as exc:
			self.setException(exc)
			absorbException(exc)
		else:
			self.setResult(result)

class Instant(CompletionFuture):
	def __new__(cls, result = None, exception = None) -> Self:
		self = super().__new__(cls)
		if exception is not None:
			self._setException(exception)
		else:
			self._setResult(result)
		return self

async def _wrapAwaitable(awaitable : Awaitable[A]) -> A:
	return await awaitable

class Task(CompletionFuture[A]):
	"""
	Performs an await operation and returns the result.
	"""

	__coro : Coroutine[Delayer,None,A]
	def __new__(cls, coro : Awaitable[A]) -> CompletionFuture:
		if isinstance(coro,CompletionFuture):
			return coro
		if not isinstance(coro,Coroutine):
			# Generic Delayer is also wrapped.
			coro = _wrapAwaitable(coro)
		self = super().__new__(cls)
		self.__coro = coro
		self._advance(None)
		return self
	
	def _advance(self,_):
		try:
			result = self.__coro.send(None)
		except StopIteration as exc:
			self._setResult(exc.value)
		except BaseException as exc:
			try:
				self._setException(exc)
			except BaseException as bexc:
				absorbException(exc)
				absorbException(bexc)
				raise ExceptionGroup("",[exc,bexc])
			absorbException(exc)
		else:
			result.then(self._advance)

def aggressiveTask(coro : Awaitable[A]):
	"""
	The awaitable is awaited synchronously. Any attempt at waiting for an
	async awaitable inside will result in an exception being raised.
	"""
	if isinstance(coro,Completion):
		if not coro.done():
			raise BaseException("Cannot await incomplete completion inside aggressive task.")
	if isinstance(coro,Delayer):
		raise BaseException("Cannot await non-completion Delayer inside aggressive task.")
	if not isinstance(coro,Coroutine):
		coro = _wrapAwaitable(coro)
	toThrow = None
	while True:		
		try:
			if toThrow is None:
				result = coro.send(None)
			else:
				result = coro.throw(toThrow)
		except StopIteration as exc:
			return exc.value
		except BaseException as exc:
			raise
		else:
			try:
				aggressiveTask(result)
			except BaseException as exc:
				toThrow = exc
			else:
				toThrow = None

class AsyncDelayer(Delayer):
	"""
	A delayer which registers the callbacks to a specified asyncio event loop.
	Waiting on this blocks until the next pass of the event loop.
	"""
	__loop : asyncio.AbstractEventLoop
	def __new__(cls, loop : asyncio.AbstractEventLoop | None = None) -> Self:
		self = super().__new__(cls)
		if loop is None:
			loop = asyncio.get_event_loop()
		self.__loop = loop
		return self
	
	def then(self,proc : Callable):
		self.__loop.call_soon_threadsafe(proc, self)

class AsyncCompletion(Completion):
	"""
	A Completion that is immediately scheduled to be completed in the specified async event loop.
	This provides the same scheduling as AsyncTask, except without a result.
	"""
	def __new__(cls, loop : asyncio.AbstractEventLoop | None = None) -> Self:
		self = super().__new__(cls)
		if loop is None:
			loop = asyncio.get_event_loop()
		loop.call_soon_threadsafe(self._complete)
		return self

class AsyncTask(CompletionFuture[A]):
	"""
	Wraps an asyncio awaitable in a CompletionFuture. If it is not a future,
	it is scheduled for execution.
	"""
	def __new__(cls, asyncCoro : Awaitable[A], loop : asyncio.AbstractEventLoop | None = None) -> Self:
		self = super().__new__(cls)
		future = asyncio.ensure_future(asyncCoro, loop = loop)
		future.add_done_callback(self.__completeFromFuture)
		return self
	
	def __completeFromFuture(self, future : asyncio.Future[A]):
		if future.cancelled():
			self._setException(CancelledException())
		else:
			exc = future.exception()
			if exc is None:
				self._setResult(future.result())
			else:
				self._setException(exc)

class Gather(Generic[*T],CompletionFuture[Tuple[*T]]):
	__completions : List[CompletionFuture]
	__failFast : bool
	@overload
	def __new__(cls, a : Awaitable[A], /) -> 'Gather[A]': ...
	@overload
	def __new__(cls, a : Awaitable[A], b : Awaitable[B], /) -> 'Gather[A,B]': ...
	@overload
	def __new__(cls, a : Awaitable[A], b : Awaitable[B], c : Awaitable[C], /) -> 'Gather[A,B,C]': ...
	@overload
	def __new__(cls, a : Awaitable[A], b : Awaitable[B], c : Awaitable[C], d : Awaitable[D], /) -> 'Gather[A,B,C,D]': ...
	def __new__(cls, *completions : CompletionFuture | Coroutine, failFast : bool = True) -> Self:
		self = super().__new__(cls)
		if len(completions) == 0:
			self._setResult(())
			return self
		
		self.__completions = []
		self.__failFast = failFast
		exception = None
		doRaise = False

		for c in completions:
			if exception is not None:
				if isinstance(c, Coroutine):
					c.close()
			else:
				try:
					t = Task(c)
				except BaseException as ex:
					# This may be an interrupt!
					exception = ex
					doRaise = True
				else:
					self.__completions.append(t)
					if failFast and t.done():
						exception = t.exception()
		
		if doRaise:
			raise exception
		
		if exception is not None:
			absorbException(exception)
			self._setException(exception)
		else:
			self.__remaining = len(self.__completions)
			for c in self.__completions:
				c.then(self.__advance)
		
		return self
	
	def __advance(self, completion : CompletionFuture):
		if self.__remaining == 0:
			return

		if self.__failFast and completion.exception() is not None:
			self.__remaining = 0
			self._setException(completion.exception())
			return
				
		self.__remaining -= 1

		if self.__remaining == 0:

			exceptionList = []
			resultList = []
			
			for c in self.__completions:
				if c.exception() is not None:
					exceptionList.append(c.exception())
				else:
					resultList.append(c.result())
			
			if len(exceptionList) == 1:
				self._setException(exceptionList[0])
			elif len(exceptionList) != 0:
				self._setException(ExceptionGroup("",exceptions=exceptionList))
			else:
				self._setResult(tuple(resultList))

#class Lock(Delayer):
#	"""
#	An async implementation of a concurrent lock.
#
#	This is useful when async code must be limited to one
#	execution at a time, while still allowing it to
#	await asyncio operations.
#
#	Very niche for objects that can async change.
#	"""
#	__locked : bool
#	__inUnlockLoop : bool
#	__queue : List[Callable[[]]]
#
#	def __new__(cls) -> Self:
#		self = super().__new__(cls)
#		self.__locked = False
#		self.__inUnlockLoop = False
#		self.__queue = []
#		return self
#	
#	def then(self, proc: Callable[[Self], Any]):
#		if self.__locked:
#			self.__queue.append(proc)
#		else:
#			proc(self)
#	
#	def isLocked(self):
#		return self.__locked
#	
#	def __await__(self):
#		while self.__locked:
#			yield self
#	
#	def __enter__(self):
#		assert not self.__locked
#		self.__locked = True
#		return self
#	
#	def __exit__(self, exct, excc, excs):
#		assert self.__locked
#		self.__locked = False
#		if not self.__inUnlockLoop:
#			self.__inUnlockLoop = True
#			exceptions = []
#			try:
#				while not self.__locked and len(self.__queue) != 0:
#					t = self.__queue.pop(0)
#					try:
#						t(self)
#					except BaseException as exc:
#						exceptions.append(exc)
#			finally:
#				self.__inUnlockLoop = False
#			if len(exceptions) != 0:
#				raise ExceptionGroup("",exceptions)


from typing import *
import asyncio
import functools

A = TypeVar('A')
B = TypeVar('B')
C = TypeVar('C')
D = TypeVar('D')
T = TypeVarTuple('T')

def mustForwardException(exc : BaseException):
	return isinstance(exc, SystemExit | KeyboardInterrupt)

class Future(Generic[A]):
	"""
	A placeholder for the outcome of an asynchronous opeartion.
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
	
	def done(self):
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
	def then(self,proc : Callable):
		raise Exception("Not implemented!")
	
	def __await__(self):
		yield self
	
	def __iter__(self):
		return (yield from self.__await__())

class QueueDelayer(Delayer):
	__queue : List[Callable]
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
		self.__queue = None
		if len(exceptions) != 0:
			raise BaseExceptionGroup("",exceptions)
	
	@override
	def then(self,proc : Callable):
		if self.__queue != None:
			self.__queue.append(proc)
		else:
			proc(self)
	
	def __await__(self):
		if self.__queue != None:
			yield self

class NoDelayer(Completion):
	"""
	A delayer with no delay. Callbacks are immediately invoked.
	"""
	def __new__(cls) -> Self:
		self = super().__new__(cls)
		self._complete()
		return self

class CompletionFuture(Future[A],Completion):
	"""
	Combination of Future, and Completion. Awaiting additionally returns the result, or raises the exception.
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
	def _complete(self):
		raise Exception("_complete may not be called directly on CompletionFuture.")

	@override
	def __await__(self):
		yield from super().__await__()
		return self.result()

	def toAsyncioFuture(self, loop : asyncio.AbstractEventLoop | None = None):
		"""
		Returns an asyncio future that is completed when this completion is completed.
		"""
		if loop is None:
			loop = asyncio.get_event_loop()
		future = loop.create_future()
		self.then(functools.partial(self.copyToAsyncioFuture,future))
		return future

class CompletableFuture(CompletionFuture):
	def __new__(cls) -> Self:
		return super().__new__(cls)
	
	def setResult(self, resultValue):
		return self._setResult(resultValue)
	
	def setException(self, exceptionValue):
		return self._setException(exceptionValue)

class Instant(CompletionFuture):
	def __new__(cls, result = None, exception = None) -> Self:
		self = super().__new__(cls)
		if exception is not None:
			self._setException(exception)
		else:
			self._setResult(result)
		return self

class Task(CompletionFuture[A]):
	"""
	Performs an await operation and returns the result.
	"""
	async def __wrapAwaitable(awaitable : Awaitable[A]) -> A:
		return await awaitable

	__coro : Coroutine[Delayer,None,A]
	def __new__(cls, coro : Awaitable[A]) -> Self:
		if isinstance(coro,Completion):
			return coro
		if not isinstance(coro,Coroutine):
			# Generic Delayer is also wrapped.
			coro = Task.__wrapAwaitable(coro)
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
			finally:
				if mustForwardException(exc):
					raise exc
		else:
			result.then(self._advance)

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
		self.__loop.call_soon(proc, self)

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
		exc = future.exception()
		if exc is None:
			self._setResult(future.result())
		else:
			self._setException(exc)

class Gather(Generic[*T],CompletionFuture[Tuple[*T]]):
	__completions : List[CompletionFuture]
	@overload
	def __new__(cls, a : Awaitable[A], /) -> 'Gather[A]': ...
	@overload
	def __new__(cls, a : Awaitable[A], b : Awaitable[B], /) -> 'Gather[A,B]': ...
	@overload
	def __new__(cls, a : Awaitable[A], b : Awaitable[B], c : Awaitable[C], /) -> 'Gather[A,B,C]': ...
	@overload
	def __new__(cls, a : Awaitable[A], b : Awaitable[B], c : Awaitable[C], d : Awaitable[D], /) -> 'Gather[A,B,C,D]': ...
	def __new__(cls, *completions : CompletionFuture | Coroutine) -> Self:
		self = super().__new__(cls)
		if len(completions) == 0:
			self._setResult(())
			return self
		
		self.__completions = []
		exception = None

		for c in completions:
			if exception is not None:
				if isinstance(c, Coroutine):
					c.close()
			else:
				t = Task(c)
				self.__completions.append(t)
				if t.done():
					exception = t.exception()
		
		if exception is not None:
			self._setException(exception)
		else:
			self.__remaining = len(self.__completions)
			for c in self.__completions:
				c.then(self.__advance)
		return self
	
	def __advance(self, completion : CompletionFuture):
		if self.__remaining == 0:
			return
		exc = completion.exception()
		if exc is None:
			self.__remaining -= 1
			if self.__remaining == 0:
				self._setResult(tuple(c.result() for c in self.__completions))
		else:
			self.__remaining = 0
			self._setException(exc)

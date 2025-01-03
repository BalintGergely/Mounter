"""
Support for coroutines in mounter.

Python asyncio assumes everything is awaited at some point,
and every operation converges into the completion of a single "main" coroutine.

The API implemented by this module instead assume that everything
is completed eventually. And by eventually, we mean that there is
always an overarching context of asynchronous execution
such that by the time the context is closed, every completion
belonging to that context is done.

Therefore, nothing needs awaiting, except actual Coroutines
that still assume an awaiter.

One example of such context of asynchronous execution is implemented
by AsyncOps, which manages a context in the form of a Workspace module.
"""

from typing import *
import asyncio
import functools

A = TypeVar('A')
B = TypeVar('B')
C = TypeVar('C')
D = TypeVar('D')
E = TypeVar('E')
P = ParamSpec('P')
T = TypeVarTuple('T')

class BaseCompletionException(Exception):
	pass

class CancelledException(BaseCompletionException):
	pass

class InterruptedException(BaseCompletionException):
	def __init__(self, cause: Exception):
		super().__init__()
		self.__cause = cause

def absorbException(exc : BaseException):
	"""
	If the argument reports SystemExit or KeyboardInterrupt, the corresponding exception is raised.
	"""
	if isinstance(exc,BaseExceptionGroup):
		for x in exc.exceptions:
			absorbException(x)
	if isinstance(exc, SystemExit | KeyboardInterrupt):
		raise exc
	return exc

def wouldAbsorbRaise(exc : BaseException):
	"""
	Returns true if 'absorbException' would raise from the specified exception.
	"""
	if isinstance(exc,BaseExceptionGroup):
		if any(wouldAbsorbRaise(x) for x in exc.exceptions):
			return True
	return isinstance(exc, SystemExit | KeyboardInterrupt)

def isInterrupt(exc : BaseException):
	"""
	Tests whether the exception is being caused by an event that occurred
	out of scope.
	"""
	if isinstance(exc,BaseExceptionGroup):
		for x in exc.exceptions:
			if not isInterrupt(x):
				return False
	return isinstance(exc, SystemExit | KeyboardInterrupt | CancelledException | InterruptedException)

def wrapException(exc : BaseException):
	"""
	Moves the specified exception out of scope for the purposes
	of absorbing. Use this to avoid cascading absorb failures.
	"""
	if wouldAbsorbRaise(exc):
		return InterruptedException(exc)
	return exc

async def awaitableToCoroutine(awaitable : Awaitable[A]) -> A:
	"""
	A simple coroutine performing an await operation on the specified awaitable.
	"""
	return await awaitable

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
	
	def failed(self):
		if not self.done():
			return False
		return self.exception() is not None
		
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
	
	def __await__(self):
		if not self.done():
			yield self
		return self.result()
	
	def __iter__(self):
		return (yield from self.__await__())

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
	
	def thenCall(self,proc : Callable[P, None],*args : P.args,**kwargs : P.kwargs):
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
		"""
		Invokes the procedure as soon as this Completion is
		completed with this Completion as the only argument.

		Will invoke immediately if already completed.
		"""
		if self.__queue != None:
			self.__queue.append(proc)
		else:
			proc(self)
	
	def __await__(self):
		if self.__queue != None:
			yield self

class CompletionFuture(Future[A],Completion,Awaitable[A]):
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
		
	def _minimal(self):
		newCompletion = CompletionFuture()
		self.then(newCompletion._copyFrom)
		return newCompletion

	def _callAndSetResult(self, proc : Callable[[*T],A], *args : *T, **kwargs):
		try:
			result = proc(*args,**kwargs)
		except BaseException as exc:
			self._setException(wrapException(exc))
			absorbException(exc)
		else:
			self._setResult(result)
		
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
	
	def thenComplete(self, target : 'CompletableFuture'):
		self.then(target.copyFrom)

class CompletableFuture(CompletionFuture[A]):
	"""
	A CompletableFuture is an open CompletionFuture. At any point
	it may be completed by a caller.

	It also supports the context manager protocol, (with) setting an exception
	if it has not been completed before exiting the context.
	"""
	def __new__(cls) -> Self:
		return super().__new__(cls)
	
	setResult = CompletionFuture._setResult
	setException = CompletionFuture._setException
	copyFrom = CompletionFuture._copyFrom
	minimal = CompletionFuture._minimal
	callAndSetResult = CompletionFuture._callAndSetResult
	
	def __enter__(self):
		return self

	def __exit__(self, exct, excc, excs):
		if not self.done():
			assert excc is not None
			self.setException(excc)

class Instant(CompletionFuture[A]):
	def __new__(cls, result : A = None, exception = None) -> Self:
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

	__coro : Coroutine[Delayer,None,A]
	def __new__(cls, coro : Awaitable[A]) -> CompletionFuture[A]:
		if isinstance(coro,CompletionFuture):
			return coro
		if isinstance(coro,Lazy):
			return coro.start()
		if not isinstance(coro,Coroutine):
			# Generic Delayer is also wrapped.
			coro = awaitableToCoroutine(coro)
		self = super().__new__(cls)
		self.__coro = coro
		self._advance(None)
		return self
	
	def _advance(self,_):
		exception = None
		while True:
			try:
				if exception is not None:
					result = self.__coro.throw(exception)
				else:
					result = self.__coro.send(None)
			except StopIteration as exc:
				self._setResult(exc.value)
				return
			except BaseException as exc:
				try:
					self._setException(wrapException(exc))
				except BaseException as bexc:
					absorbException(exc)
					absorbException(bexc)
					raise ExceptionGroup("",exceptions = [exc,bexc])
				else:
					absorbException(exc)
				return
			else:
				if isinstance(result,Delayer):
					result.then(self._advance)
					return
				else:
					exception = Exception(f"Awaiting {type(result)} is not supported!")

class Lazy(Future[A],Delayer):
	"""
	Represents a background computation that may possibly not be started.
	Calling the start method or awaiting will start the computation.
	This is not a CompletionFuture.

	Use with Gather to avoid starting multiple tasks when one fails early.
	"""
	__proc : Callable[...,Awaitable[A]]
	__task : Delayer | None
	def __new__(cls, fun : Callable[P,Awaitable[A]], *args : P.args, **kwargs : P.kwargs) -> 'Lazy[A]':
		self = super().__new__(cls)
		self.__proc = functools.partial(fun, *args, **kwargs)
		self.__task = None
		return self
	
	def start(self) -> CompletionFuture[A]:
		"""
		Starts the background computation without blocking, and returns a CompletionFuture
		representing the computation.
		"""
		if self.__task is None:
			try:
				self.__task = Task(self.__proc())
			except BaseException as ex:
				self.__task = Instant(exception = wrapException(ex))
				absorbException(ex)
			finally:
				self.__task.then(self._copyFrom)
		return self.__task
	
	__call__ = start
	
	@override
	def then(self, proc):
		self.start().thenCall(proc, self)
	
	@override
	def thenCall(self,proc : Callable[P, None],*args : P.args,**kwargs : P.kwargs):
		self.start().thenCall(proc, *args, **kwargs)

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
		coro = awaitableToCoroutine(coro)
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
	__future : asyncio.Future
	def __new__(cls, asyncCoro : Awaitable[A], loop : asyncio.AbstractEventLoop | None = None) -> Self:
		self = super().__new__(cls)
		self.__future = asyncio.ensure_future(asyncCoro, loop = loop)
		if self.__future.done():
			self.__completeFromAsyncioFuture(self.__future)
		else:
			self.__future.add_done_callback(self.__completeFromAsyncioFuture)
		return self

	def __completeFromAsyncioFuture(self, future : asyncio.Future[A]):
		if future.cancelled():
			self._setException(CancelledException())
		else:
			exc = future.exception()
			if exc is None:
				self._setResult(future.result())
			else:
				self._setException(exc)

def completePolicy(*fut : Future):
	"""
	Gather completes when all futures are done.
	Result will be tuple of results if all succeeded.
	If any failed, Gather fails.
	"""
	if all(k.done() for k in fut):
		if any(k.failed() for k in fut):
			exc = ExceptionGroup("", (k.exception() for k in fut if k.failed()))
			return Instant(exception = exc)
		return Instant(result = tuple(k.result() for k in fut))
	return None

def tuplePolicy(*fut : Future):
	"""
	Gather completes when all futures are done with a tuple of results.
	If any fails, Gather completes with the first exception.
	"""
	for f in filter(Future.failed,fut): return f
	if all(k.done() for k in fut):
		return Instant(result = tuple(k.result() for k in fut))
	return None

def cancelPolicy(core : Future[A], cancel : Future) -> Future[A]:
	"""
	Gather completes when the first future completes with the result of the first future.
	If the second future fails sooner, Gather also does so.
	"""
	if cancel.failed():
		return cancel
	if core.done():
		return core
	return None

def orPolicy(*fut : CompletionFuture):
	"""
	Performs a disjunction over futures.
	Gather completes early if any complete with True.
	"""
	for f in filter(lambda f: f.done() and f.result(), fut): return f
	for f in filter(Future.failed,fut): return f
	if all(k.done() for k in fut):
		return Instant(False)
	return None

def andPolicy(*fut : CompletionFuture):
	"""
	Performs a conjunction over futures.
	Gather completes early if any complete with False.
	"""
	for f in filter(lambda f: f.done() and not f.result(), fut): return f
	for f in filter(Future.failed,fut): return f
	if all(k.done() for k in fut):
		return Instant(True)
	return None

class Gather(CompletionFuture[A]):
	__completions : List[CompletionFuture]
	__pendingCallsToAdvance : int
	__policy : Callable
	def __new__(
			cls,
			*completions : CompletionFuture | Coroutine,
			policy : Callable[[*T],Future[A]] = tuplePolicy,
			failFast : bool = False) -> 'Gather[A]':
		self = super().__new__(cls)

		self.__completions = []

		exception = None
		failed = False

		for c in completions:
			if failed:
				if isinstance(c, Coroutine):
					c.close()
			else:
				try:
					t = Task(c)
				except BaseException as ex:
					# This may be an interrupt!
					exception = ex
					failed = True
				else:
					self.__completions.append(t)
					failed = failFast and t.failed()
		
		if exception is not None:
			raise exception

		self.__policy = policy

		self.__pendingCallsToAdvance = 1

		for t in self.__completions:
			if not t.done():
				self.__pendingCallsToAdvance += 1
				t.then(self.__advance)
		
		self.__advance(None)

		return self

	def __advance(self, _):
		if self.__pendingCallsToAdvance == 0:
			return

		self.__pendingCallsToAdvance -= 1

		try:
			fin : Awaitable | None = self.__policy(*self.__completions)
		except BaseException as exc:
			self.__pendingCallsToAdvance = 0
			self._setException(wrapException(exc))
			absorbException(exc)
		else:
			if fin is None:
				if self.__pendingCallsToAdvance == 0:
					self._setException(Exception("Ran out of things to wait for without producing a result."))
			else:
				self.__pendingCallsToAdvance = 0
				Task(fin).then(self._copyFrom)

@overload
def gather(a : Awaitable[A]) -> Awaitable[Tuple[A]]: ...
@overload
def gather(a : Awaitable[A], b : Awaitable[B]) -> Awaitable[Tuple[A,B]]: ...
@overload
def gather(a : Awaitable[A], b : Awaitable[B], c : Awaitable[C]) -> Awaitable[Tuple[A,B,C]]: ...
@overload
def gather(a : Awaitable[A], b : Awaitable[B], c : Awaitable[C], d : Awaitable[D]) -> Awaitable[Tuple[A,B,C,D]]: ...
@overload
def gather(a : Awaitable[A], b : Awaitable[B], c : Awaitable[C], d : Awaitable[D], e : Awaitable[E]) -> Awaitable[Tuple[A,B,C,D,E]]: ...

def gather(*c : Awaitable) -> Awaitable:
	return Gather(*c, policy = tuplePolicy, failFast = True)

def gatherOr(*c : Awaitable[bool]) -> Awaitable[bool]:
	return Gather(*c, policy = orPolicy)

def gatherAnd(*c : Awaitable[bool]) -> Awaitable[bool]:
	return Gather(*c, policy = andPolicy)

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

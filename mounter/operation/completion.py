"""
Support for coroutines in mounter.

Python asyncio assumes everything is awaited at some point,
and every operation converges into the completion of a single "main" coroutine.

The API implemented by this module instead assume that everything
is completed eventually. And by eventually, we mean that there is
always an overarching context of asynchronous execution
such that by the time the context is closed, every completion
belonging to that context is done.

The main reason for this behaviour is the enabling of short-circuiting
joint wait operations. For instance starting a set of operations and
waiting until one of them evaluates to True to proceed. The wait can
finish earlier while the remaining operations still have a guarantee
of completion.

The secondary reason for the existence of this module is to have a
reliable way to turn everything sequential and deterministically ordered
at the flip of a switch.
"""

from typing import *
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
		"""
		Returns True if this future is done, and has completed exceptionally. False otherwise.
		"""
		if not self.done():
			return False
		return self.exception() is not None
	
	def succeeded(self):
		if not self.done():
			return False
		return self.exception() is None
		
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
	
	def __await__(self) -> Generator['Delayer']:
		yield self
	
	def __iter__(self):
		return (yield from self.__await__())

class DelayerMethod(Delayer):
	def __init__(self, fun : Callable[[], None]):
		self.__fun = fun
	
	def __set_name__(self, owner, name):
		self.__name = name
	
	def __get__(self, target, *args, **kwargs):
		delayerObject = DelayerMethod(self.__fun.__get__(target, *args, **kwargs))
		setattr(target, self.__name, delayerObject)
		return delayerObject
	
	def thenCall(self, proc, *args, **kwargs):
		self.__fun(functools.partial(proc,*args,**kwargs))
	
	def then(self, proc : Callable[[Self], None]):
		self.thenCall(proc,self)

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
	"""
	A delayer adding callbacks to a queue and offering a run method
	to manually run a specified number of callbacks.

	Any user of QueueDelayer is RESPONSIBLE for ensuring the queue
	will be eventually fully drained.
	"""
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

	def fromDelayer(d : Delayer):
		c = Completion()
		d.thenCall(c._complete)
		return c
	
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
		
	def minimal(self):
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

	def thenComplete(self, target : 'CompletableFuture'):
		self.then(target.copyFrom)

class CompletableFuture(CompletionFuture[A]):
	"""
	A CompletableFuture is an open CompletionFuture. At any point
	it may be completed by a caller.

	It also supports the context manager protocol, (with) setting an exception
	if it has not been completed before exiting the context.
	"""	
	setResult = CompletionFuture._setResult
	setException = CompletionFuture._setException
	copyFrom = CompletionFuture._copyFrom
	callAndSetResult = CompletionFuture._callAndSetResult
	
	def __enter__(self):
		return self

	def __exit__(self, exct, excc, excs):
		if not self.done():
			if excc is None:
				self.setException(Exception("CompletableFuture exited without setting a result"))
			else:
				self.setException(excc)

class Instant(CompletionFuture[A]):
	def __new__(cls, result : A = None, exception = None) -> CompletionFuture:
		self = super().__new__(cls)
		if exception is not None:
			self._setException(exception)
		else:
			self._setResult(result)
		return self

INSTANT : Final[CompletionFuture[None]] = Instant()

class Task(CompletionFuture[A]):
	"""
	Performs an await operation and returns the result.
	"""

	__coro : Coroutine[Delayer,None,A]
	def __new__(cls, coro : Awaitable[A], startDelay = INSTANT) -> CompletionFuture[A]:
		if isinstance(coro,CompletionFuture):
			if startDelay is not INSTANT:
				return coro.withDelay(startDelay)
			return coro
		if isinstance(coro,Lazy):
			return coro.start()
		if not isinstance(coro,Coroutine):
			# Generic Delayer is also wrapped.
			coro = awaitableToCoroutine(coro)
		self = super().__new__(cls)
		self.__coro = coro
		startDelay.then(self._advance)
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

def fftask(coro : Awaitable[A]) -> CompletionFuture[A]:
	"""
	Fail-fast task. Wraps the Awaitable in a Task, but if it
	fails immediately, the exception is raised from fftask.
	"""
	coro = Task(coro)
	if coro.failed():
		raise coro.exception()
	return coro

def instantCall(target : Callable[[],A], *args, **kwargs) -> Instant[A]:
	"""
	Call a function and return an Instant with the result.
	"""
	try:
		return Instant(result = target(*args,**kwargs))
	except BaseException as exc:
		return Instant(exception = exc)

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

def completePolicy(group : 'Gather', *_):
	"""
	Gather completes when all futures are done.
	Result will be tuple of results if all succeeded.
	If any failed, Gather fails.
	"""
	if group.remaining() == 0:
		if any(k.failed() for k in group.futures()):
			exc = ExceptionGroup("", (k.exception() for k in group.futures() if k.failed()))
			return Instant(exception = exc)
		return Instant(result = tuple(k.result() for k in group.futures()))
	return None

def tuplePolicy(group : 'Gather', *latest : Future):
	"""
	Gather completes when all futures are done with a tuple of results.
	If any fails, Gather completes with the first exception.
	"""
	for l in latest:
		if l.failed():
			return l
	if group.remaining() == 0:
		return Instant(result = tuple(k.result() for k in group.futures()))
	return None

def orPolicy(group : 'Gather', *latest : Future):
	"""
	Performs a disjunction over futures.
	Gather completes early if any complete with True.
	"""
	for l in latest:
		if l.failed() or l.result():
			return l
	if group.remaining() == 0:
		return Instant(False)
	return None

def andPolicy(group : 'Gather', *latest : Future):
	"""
	Performs a conjunction over futures.
	Gather completes early if any complete with False.
	"""
	for l in latest:
		if l.failed() or not l.result():
			return l
	if group.remaining() == 0:
		return Instant(True)
	return None

class Gather(CompletionFuture[A]):
	__completions : List[CompletionFuture]
	__pendingCallsToAdvance : int
	__policy : Callable
	def __new__(
			cls,
			*completions : CompletionFuture | Coroutine,
			policy : Callable[['Gather',Future],Future[A]] = tuplePolicy,
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

		self.__completions = tuple(self.__completions)

		self.__policy = policy

		self.__pendingCallsToAdvance = 1

		alreadyCompleted = []

		for t in self.__completions:
			if t.done():
				alreadyCompleted.append(t)
			else:
				self.__pendingCallsToAdvance += 1
				t.then(self.__advance)
		
		self.__advance(*alreadyCompleted)

		return self
	
	def remaining(self):
		return self.__pendingCallsToAdvance

	def futures(self):
		return self.__completions

	def __advance(self, *latest):
		if self.__pendingCallsToAdvance == 0:
			return

		self.__pendingCallsToAdvance -= 1

		try:
			fin : Awaitable | None = self.__policy(self, *latest)
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
#	async def __aenter__(self):
#		await self
#		self.__locked = True
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
#
#	async def __aexit__(self, exct, excc, excs):
#		self.__exit__(exct,excc,excs)
#


import asyncio
import subprocess
import functools
from typing import TypeVar, TypeVarTuple, Tuple, Generic, Awaitable, Callable, Type, Iterator, Coroutine, List, overload
from mounter.path import Path
from mounter.workspace import Module, ModuleInitContext, Workspace
from mounter.progress import Progress
from mounter.persistence import Persistence
from mounter.delta import FileDeltaChecker

A = TypeVar('A')
B = TypeVar('B')
C = TypeVar('C')
D = TypeVar('D')
T = TypeVarTuple('T')

class _asyncOpsTask():
	def __init__(self,
			coro : Coroutine,
			asyncOps : 'AsyncOps',
			activateNow = False,
			eagerStart = False,
			escapeAllowed = False) -> None:
		self.__asyncOps = asyncOps
		self.__coro = coro
		self.__isActive = False
		"""
		Active tasks participate in the event loop, whereas inactive
		tasks are dormant.

		A task becomes active if it is awaited by an active task, or
		given as argument to AsyncOps.completeNow.
		"""
		self.__result = None
		self.__escapeAllowed = escapeAllowed
		self.__blockingObject = None
		"""
		The blocking object.
		"""
		self.__blocked : List[Callable] = []
		if eagerStart:
			self.__advance()
		if activateNow:
			self._activate()
	
	def __getLoop(self):
		return self.__asyncOps._getLoop()
	
	def _finalize(self):
		self.__coro.close()
		return self.__isActive
	
	def _isActive(self):
		return self.__isActive
	
	def _runLoopUntilDone(self):
		if self._isDone():
			return
		loop = self.__getLoop()
		self._callWhenDone(loop.stop)
		loop.run_forever()
	
	def __setResult(self, value = None, exception = None):
		if exception is not None:
			self.__result = (True,exception)
		else:
			self.__result = (False,value)
		for t in self.__blocked:
			t()
		self.__blocked = ()

	def _enactResult(self):
		(doRaise,what) = self.__result
		if doRaise:
			raise what
		else:
			return what
	
	def _isDone(self):
		return self.__result is not None

	def _callWhenDone(self, doneCallback : Callable):
		"""
		The callback is called as soon as this task is done.
		It may be called immediately. It receives no arguments.

		This task is also activated.
		"""
		self._activate()
		if self._isDone():
			doneCallback()
		else:
			self.__blocked.append(doneCallback)
	
	def __unblock(self, *_):
		"""
		Clears the blocking object. If active, also schedules an advance.
		"""
		self.__blockingObject = None
		if self.__isActive:
			self.__getLoop().call_soon(self.__advance)

	def __advance(self):
		try:
			result = self.__coro.send(None)
		except StopIteration as exc:
			self.__setResult(exc.value)
		except (KeyboardInterrupt, SystemExit) as exc:
			self.__setResult(exception = exc)
			raise
		except BaseException as exc:
			self.__setResult(exception = exc)
		else:
			if self.__escapeAllowed:
				assert isinstance(result, _asyncOpsTask | asyncio.Future | None)
			else:
				assert isinstance(result, _asyncOpsTask | None)
			self.__blockingObject = result
			if self.__isActive:
				self.__activateUnblock()
	
	def _activate(self) -> Awaitable[A]:
		if not self.__isActive:
			self.__isActive = True
			if not self._isDone(): # Can happen if eager start also finishes.
				self.__activateUnblock()
	
	def __activateUnblock(self):
		if self.__blockingObject is None:
			self.__unblock()
		elif isinstance(self.__blockingObject, _asyncOpsTask):
			self.__blockingObject._callWhenDone(self.__unblock)
		elif isinstance(self.__blockingObject, asyncio.Future):
			self.__blockingObject.add_done_callback(self.__unblock)
		else:
			raise Exception("Illegal blocking object!")
	
	def __await__(self):
		yield self
		return self._enactResult()

	__iter__ = __await__

async def awaitAll(*args):
	return tuple([await t for t in args])

class AsyncOps(Module):
	"""
	Module for async execution.

	As a rule, ALL tasks created must be awaited at some point.

	DO NOT use asyncio! It is NOT COMPATIBLE!
	"""
	def __init__(self, context) -> None:
		super().__init__(context)
		self.__runner = asyncio.Runner()
		self.__asyncEnabled = True
		self.__asyncOpList : List[_asyncOpsTask] = []
	
	def disableAsync(self):
		self.__asyncEnabled = False

	def run(self):
		with self.__runner:
			success = False
			try:
				self._downstream()
				success = True
			finally:
				notActiveCounter = 0
				for op in self.__asyncOpList:
					if not op._finalize():
						notActiveCounter += 1
				if notActiveCounter != 0 and success:
					print(f"Warning: {notActiveCounter} tasks were never awaited!")
	
	def _getLoop(self):
		return self.__runner.get_loop()

	def __createTask(self,coro : Coroutine[None,None,A], lazy = False, eagerStart = True, escapeAllowed = False) -> Awaitable[A]:
		assert asyncio.iscoroutine(coro)
		wrapper = _asyncOpsTask(
			coro,
			self,
			activateNow = (not lazy) and self.__asyncEnabled,
			eagerStart = eagerStart,
			escapeAllowed = escapeAllowed)
		self.__asyncOpList.append(wrapper)
		return wrapper
	
	def createTask(self,coro : Coroutine[None,None,A], lazy = False) -> Awaitable[A]:
		return self.__createTask(coro, lazy = lazy)
	
	def ensureFuture(self,coro : Awaitable[A]) -> Awaitable[A]:
		if isinstance(coro,_asyncOpsTask):
			return coro
		else:
			return self.createTask(coro)

	def completeNow(self,task : Awaitable[A]) -> A:
		"""
		Block until the specified Awaitable is done. Return it's result or raise it's exception.
		"""
		task : _asyncOpsTask = self.ensureFuture(task)
		task._runLoopUntilDone()
		return task._enactResult()
	
	def completeLater(self,task : Awaitable[A]) -> Awaitable[A]:
		"""
		Schedules the awaitable to be completed after all modules are loaded.
		"""
		task : _asyncOpsTask = self.ensureFuture(task)
		self.ws.append(lambda: task._runLoopUntilDone())
		return task
		
	def runCommand(self, command, input = bytes(), expectedReturnCode = 0) -> Awaitable[Tuple[int, bytes, bytes]]:
		# This is a coroutine-like which means it is definitely lazy. No eager start. Starts when first awaited.
		return self.__createTask(
			self.__runCommandImpl(command,input,expectedReturnCode),
			lazy = True,
			eagerStart = False,
			escapeAllowed = True
		)
	
	async def __runCommandImpl(self, command, input = bytes(), expectedReturnCode = 0) -> Tuple[int, bytes, bytes]:
		commandStr = subprocess.list2cmdline(str(c) for c in command)

		proc = await asyncio.create_subprocess_shell(commandStr,
			stdout=asyncio.subprocess.PIPE,
			stderr=asyncio.subprocess.PIPE,
			stdin=asyncio.subprocess.PIPE)

		stdout, stderr = await proc.communicate(input)

		if stderr != b'':
			print(f"Error output: {commandStr}")
			print(stderr.decode(), end="")

		assert proc.returncode == expectedReturnCode
		
		return (stdout, stderr)
	
	@overload
	def gather(self, a:Awaitable[A], /) -> Awaitable[Tuple[A]]: ...
	@overload
	def gather(self, a:Awaitable[A],b:Awaitable[B], /) -> Awaitable[Tuple[A,B]]: ...
	@overload
	def gather(self, a:Awaitable[A],b:Awaitable[B],c:Awaitable[C], /) -> Awaitable[Tuple[A,B,C]]: ...
	@overload
	def gather(self, a:Awaitable[A],b:Awaitable[B],c:Awaitable[C],d:Awaitable[D], /) -> Awaitable[Tuple[A,B,C,D]]: ...
	def gather(self, *a):
		return self.createTask(awaitAll(*[self.ensureFuture(t) for t in a]))

class instant(Generic[A],Awaitable[A]):
	def __init__(self, value : A) -> None:
		self.__value = value
	
	def __next__(self): raise StopIteration(self.__value)
	def __await__(self): return self
	def __iter__(self): return self
	def send(self, input): assert input is None; raise StopIteration(self.__value)

def manifest() -> Type[AsyncOps]:
	return AsyncOps

# Don't forget asyncio.gather which is super useful

def once(fun : Callable[[*T],A]) -> Callable[[*T],A]:
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

def task(coro : Callable[[*T],Awaitable[A]]) -> Callable[[*T],Awaitable[A]]:
	"""
	The decorated coroutine is wrapped in an AsyncOps task.
	AsyncOps instance is obtained through the "ws" member of self.
	Watch out for hanging tasks!
	"""
	@functools.wraps(coro)
	def wrapper(self,*args,**kwargs):
		ws : Workspace = self.ws
		return ws[AsyncOps].ensureFuture(coro(self,*args,**kwargs))
	return wrapper

def op(arg):
	return once(task(arg))

class __light():
	def __init__(self) -> None:
		pass

	def __await__(self):
		yield None
	
	__iter__ = __await__

RED_LIGHT = __light()
"""Wait on this before any synchronous operation."""

class CopyOps(Module):
	def __init__(self, context) -> None:
		super().__init__(context)
		self.ws.add(AsyncOps)
		self.ws.add(Progress)
	
	def run(self):
		return super().run()
	
	@op
	async def copyFile(self, sourcePath : Path, targetPath : Path):
		with self.ws[Progress].register() as pu:
			pu.setName(f"Copy {sourcePath} to {targetPath}")
			deltaChecker = self.ws[FileDeltaChecker]
			self.ws[Persistence].lookup(self)
			data = self.getFileProperties(targetPath)
			if deltaChecker.query(sourcePath) != data.get("sourceHash") \
			or not targetPath.isPresent():
				pu.setRunning()
				sourcePath.opCopyTo(targetPath)
			else:
				pu.setUpToDate()

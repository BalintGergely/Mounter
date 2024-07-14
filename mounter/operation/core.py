
import asyncio
import subprocess
import functools
from typing import TypeVar, TypeVarTuple, Tuple, Generic, Awaitable, Callable, Type, Iterator, Coroutine, List
from mounter.path import Path
from mounter.workspace import Module, ModuleInitContext, Workspace
from mounter.progress import Progress
from mounter.persistence import Persistence
from mounter.delta import FileDeltaChecker

A = TypeVarTuple('A')
T = TypeVar('T')

class _asyncOpsTaskWrapper(Generic[T]):
	def __init__(self, coro : Awaitable[T], loop, startNow) -> None:
		self.__loop = loop
		self.__isActive = False
		self.__task = coro
		if startNow:
			self._activate()
	
	def _finalize(self):
		if not self.__isActive:
			if isinstance(self.__task,Coroutine):
				self.__task.close()
			return False
		return True
	
	def _isActive(self):
		return self.__isActive
	
	def _activate(self) -> Awaitable[T]:
		if not self.__isActive:
			self.__task = asyncio.ensure_future(self.__task, loop = self.__loop)
			self.__isActive = True
		return self.__task
	
	def __await__(self):
		self._activate()
		return (yield from self.__task)

	__iter__ = __await__

	def __del__(self):
		if isinstance(self.__task,Coroutine):
			self.__task.close()

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
		self.__asyncOpList : List[_asyncOpsTaskWrapper] = []
	
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
	
	def __getLoop(self):
		return self.__runner.get_loop()
	
	def createTask(self,coro : Awaitable[T]) -> Awaitable[T]:
		assert not isinstance(coro,_asyncOpsTaskWrapper)
		wrapper = _asyncOpsTaskWrapper(coro, self.__getLoop(), self.__asyncEnabled)
		self.__asyncOpList.append(wrapper)
		return wrapper
	
	def ensureFuture(self,coro : Awaitable[T]) -> Awaitable[T]:
		if isinstance(coro,_asyncOpsTaskWrapper):
			return coro
		else:
			return self.createTask(coro)

	def completeNow(self,task : Awaitable[T]) -> T:
		"""
		Block until the specified Awaitable is done. Return it's result or raise it's exception.
		"""
		task : _asyncOpsTaskWrapper = self.ensureFuture(task)
		asyncFuture = task._activate()
		return self.__getLoop().run_until_complete(asyncFuture)
	
	def completeLater(self,task : Awaitable[T]) -> Awaitable[T]:
		"""
		Schedules the awaitable to be completed after all modules are loaded.
		"""
		task = self.ensureFuture(task)
		self.ws.append(lambda: self.completeNow(task))
		return task

	async def runCommand(self, command, input = bytes(), expectedReturnCode = 0) -> Tuple[int, bytes, bytes]:
		commandStr = subprocess.list2cmdline(str(c) for c in command)

		proc = await asyncio.create_subprocess_shell(commandStr,
			stdout=asyncio.subprocess.PIPE,
			stderr=asyncio.subprocess.PIPE,
			stdin=asyncio.subprocess.PIPE)

		stdout, stderr = await proc.communicate(input)

		if stdout != b'' or stderr != b'':
			print(f"Output: {commandStr}")
			print(stdout.decode(), end="")
			print(stderr.decode(), end="")

		assert proc.returncode == expectedReturnCode
		
		return (stdout, stderr)
	
	def gather(self, *commandSeq : Awaitable) -> Awaitable[Tuple[*A]]:
		return self.createTask(awaitAll(*[self.ensureFuture(t) for t in commandSeq]))

class instant(Generic[T],Awaitable[T]):
	def __init__(self, value : T) -> None:
		self.__value = value
	
	def __next__(self): raise StopIteration(self.__value)
	def __await__(self): return self
	def __iter__(self): return self
	def send(self, input): assert input is None; raise StopIteration(self.__value)

def manifest() -> Type[AsyncOps]:
	return AsyncOps

# Don't forget asyncio.gather which is super useful

def once(fun : Callable[[*A],T]) -> Callable[[*A],T]:
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

def task(coro : Callable[[*A],Awaitable[T]]) -> Callable[[*A],Awaitable[T]]:
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

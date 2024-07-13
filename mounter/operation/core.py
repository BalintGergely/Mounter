
import asyncio
import subprocess
import functools
from typing import TypeVar, TypeVarTuple, Tuple, Generic, Awaitable, Callable, Type
from mounter.path import Path
from mounter.workspace import Module, ModuleInitContext, Workspace
from mounter.progress import Progress
from mounter.persistence import Persistence
from mounter.delta import FileDeltaChecker
from asyncio import gather, Future, Task

A = TypeVarTuple('A')
T = TypeVar('T')

class Asyncio(Module):
	"""
	Module that sets up an asyncio event loop.

	As a rule, modules should not leave async tasks
	hanging when they exit, because those tasks
	can outlive other modules that rely on there
	being no active async tasks.

	Either cancel the tasks or ensure they are completed
	with run_until_complete.
	"""
	def __init__(self, context) -> None:
		super().__init__(context)
		self.__runner = asyncio.Runner()
	
	def run(self):
		with self.__runner:
			self._downstream()
	
	def getLoop(self):
		return self.__runner.get_loop()
		
	def createTask(self,coro):
		return self.getLoop().create_task(coro)
	
	def completeLater(self,task : Awaitable):
		"""
		Schedules the awaitable to be completed after all modules are loaded.
		"""
		task = asyncio.ensure_future(task)
		self.ws.append(lambda: self.getLoop().run_until_complete(task))
		return task

	def completeNow(self,task : Awaitable):
		"""
		Block until the specified Awaitable is done. Return it's result or raise it's exception.
		"""
		task = asyncio.ensure_future(task)
		return self.getLoop().run_until_complete(task)

	async def runCommand(self, command, input = bytes()) -> Tuple[int, bytes, bytes]:
		commandStr = subprocess.list2cmdline(str(c) for c in command)

		proc = await asyncio.create_subprocess_shell(commandStr,
			stdout=asyncio.subprocess.PIPE,
			stderr=asyncio.subprocess.PIPE,
			stdin=asyncio.subprocess.PIPE)

		stdout, stderr = await proc.communicate(input)

		print(stdout.decode(), end="")
		print(stderr.decode(), end="")

		return (proc.returncode, stdout, stderr)

class instant(Generic[T],Awaitable[T]):
	def __init__(self, value : T) -> None:
		self.__value = value
	
	def __next__(self): raise StopIteration(self.__value)
	def __await__(self): return self
	def __iter__(self): return self
	def send(self, input): assert input is None; raise StopIteration(self.__value)

def manifest() -> Type[Asyncio]:
	return Asyncio

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
	The decorated coroutine is wrapped in an asyncio task. Watch out for hanging tasks!
	"""
	@functools.wraps(coro)
	def wrapper(*args,**kwargs):
		return asyncio.ensure_future(coro(*args,**kwargs))
	return wrapper

def op(arg):
	return once(task(arg))

class CopyOps(Module):
	def __init__(self, context) -> None:
		super().__init__(context)
		self.ws.add(Asyncio)
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

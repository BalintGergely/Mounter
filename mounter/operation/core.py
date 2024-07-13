
import asyncio
import subprocess
import functools
from typing import TypeVar, TypeVarTuple, Tuple, Generic, Awaitable, Callable, Type, Iterator
from mounter.path import Path
from mounter.workspace import Module, ModuleInitContext, Workspace
from mounter.progress import Progress
from mounter.persistence import Persistence
from mounter.delta import FileDeltaChecker

A = TypeVarTuple('A')
T = TypeVar('T')

class CommandFail(BaseException):
	def __init__(self, command : str, returnCode : int, stdout : bytes, stderr : bytes, expectedReturnCode : int = 0) -> None:
		self.__command = command
		self.__code = returnCode
		self.__stdout = stdout
		self.__stderr = stderr
		self.__expected = expectedReturnCode
	
	def printDetails(self):
		print(f"Fail: {self.__command}")
		if self.__expected == 0:
			print(f"Exited with code {self.__code}")
		else:
			print(f"Exited with code {self.__code} (Expected {self.__expected})")
		print(self.__stdout.decode(), end="")
		print(self.__stderr.decode(), end="")
	
	def __str__(self):
		return "CommandFail(...)"
	
	def __repr__(self) -> str:
		return "CommandFail(...)"

class AsyncOps(Module):
	"""
	Module for async execution.

	As a rule, modules should not leave async tasks
	hanging when they exit, because those tasks
	can outlive other modules that rely on there
	being no active async tasks.

	Either cancel the tasks or ensure they are completed
	with run_until_complete.

	DO NOT use asyncio! It is NOT ALWAYS COMPATIBLE!
	"""
	def __init__(self, context) -> None:
		super().__init__(context)
		self.__runner = asyncio.Runner()
		self.__remainingCommandFails = 1
	
	def __handleCommandFail(self, ex : CommandFail):
		if self.__remainingCommandFails != 0:
			self.__remainingCommandFails -= 1
			ex.printDetails()
	
	def __handleException(self, loop, context):
		if "exception" in context:
			ex = context["exception"]
			if isinstance(ex,CommandFail):
				return self.__handleCommandFail()
		self.__runner.get_loop().default_exception_handler(context)
	
	def run(self):
		with self.__runner:
			try:
				self.__runner.get_loop().set_exception_handler(self.__handleException)
				self._downstream()
			except CommandFail as f:
				self.__handleCommandFail(f)
	
	def __getLoop(self):
		return self.__runner.get_loop()
	
	def ensureFuture(self,coro):
		return asyncio.ensure_future(coro)
		
	def createTask(self,coro):
		return self.__getLoop().create_task(coro)
	
	def completeLater(self,task : Awaitable):
		"""
		Schedules the awaitable to be completed after all modules are loaded.
		"""
		task = asyncio.ensure_future(task)
		self.ws.append(lambda: self.__getLoop().run_until_complete(task))
		return task

	def completeNow(self,task : Awaitable):
		"""
		Block until the specified Awaitable is done. Return it's result or raise it's exception.
		"""
		task = asyncio.ensure_future(task)
		return self.__getLoop().run_until_complete(task)

	async def runCommand(self, command, input = bytes(), expectedReturnCode = 0) -> Tuple[int, bytes, bytes]:
		commandStr = subprocess.list2cmdline(str(c) for c in command)

		proc = await asyncio.create_subprocess_shell(commandStr,
			stdout=asyncio.subprocess.PIPE,
			stderr=asyncio.subprocess.PIPE,
			stdin=asyncio.subprocess.PIPE)

		stdout, stderr = await proc.communicate(input)

		if proc.returncode != expectedReturnCode:
			raise CommandFail(commandStr, proc.returncode, stdout, stderr, expectedReturnCode)
		
		if stdout != b'' or stderr != b'':
			print(f"Output: {commandStr}")
			print(stdout.decode(), end="")
			print(stderr.decode(), end="")

		return (stdout, stderr)
	
	def gather(self, *commandSeq : Awaitable) -> Awaitable[Tuple[*A]]:
		return asyncio.gather(*commandSeq)

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

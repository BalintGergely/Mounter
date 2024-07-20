
import asyncio
import subprocess
import functools
from typing import TypeVar, TypeVarTuple, Tuple, Generic, Awaitable, Callable, Type, Iterator, Coroutine, List, overload
from mounter.path import Path
from mounter.workspace import Module, ModuleInitContext, Workspace
from mounter.progress import Progress, ProgressUnit
from mounter.persistence import Persistence
from mounter.delta import FileDeltaChecker
from mounter.operation.completion import *

T = TypeVarTuple("T")

class AsyncOps(Module):
	"""
	Module for async execution.

	As a rule, ALL tasks created must be awaited at some point.

	DO NOT use asyncio! It is NOT COMPATIBLE!
	"""
	def __init__(self, context) -> None:
		super().__init__(context)
		self.__runner = asyncio.Runner()
		self.__maxParallelCommands = None
		self.__commandCount = 0
		self.__commandQueue = QueueDelayer()
		self.__redLight = None
	
	def disableAsync(self):
		self.__maxParallelCommands = 1
	
	def run(self):
		with self.__runner:
			if self.__redLight is None:
				self.__redLight = AsyncDelayer(self.__runner.get_loop())
			self._downstream()

	def __getLoop(self):
		return self.__runner.get_loop()
	
	async def __enterCommand(self):
		while self.__maxParallelCommands is not None and self.__maxParallelCommands <= self.__commandCount:
			await self.__commandQueue
		self.__commandCount += 1
	
	def __runCommands(self):
		self.__commandQueue.run(self.__maxParallelCommands - self.__commandCount)
	
	def __exitCommand(self):
		self.__commandCount -= 1
		if self.__commandQueue.waiting():
			self.__getLoop().call_soon(self.__runCommands)
	
	def __runLoopUntil(self,delay : Delayer):
		loop = self.__getLoop()
		delay.then(lambda x:loop.stop())
		loop.run_forever()
	
	def redLight(self) -> Awaitable:
		return self.__redLight
	
	def completeNow(self,coro : Awaitable[A]) -> A:
		"""
		Block until the specified Awaitable is done. Return it's result or raise it's exception.
		"""
		task = Task(coro)
		self.__runLoopUntil(task)
		return task.result()
	
	def completeLater(self,task : Awaitable[A]) -> Awaitable[A]:
		"""
		Schedules the awaitable to be completed after all modules are loaded.
		"""
		task = Task(task)
		self.ws.append(lambda: self.completeNow(task))
		
	async def runCommand(self, command, input = bytes(), *, progressUnit : ProgressUnit = None) -> Tuple[int, bytes, bytes]:
		"""
		Runs the specified command. If specified, the progressUnit is
		set to running before the command is actually started.

		Returns the exit code, the output, and error output of the command.
		"""
		commandStr = subprocess.list2cmdline(str(c) for c in command)

		await self.__enterCommand()
		try:
			if progressUnit is not None:
				progressUnit.setRunning()
			
			proc = await AsyncTask(asyncio.create_subprocess_shell(commandStr,
				stdout=asyncio.subprocess.PIPE,
				stderr=asyncio.subprocess.PIPE,
				stdin=asyncio.subprocess.PIPE))

			stdout, stderr = await AsyncTask(proc.communicate(input))
			rc = proc.returncode
		finally:
			self.__exitCommand()
		
		return (rc, stdout, stderr)

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
	def wrapper(*args,**kwargs):
		return Task(coro(*args,**kwargs))
	return wrapper

def op(arg):
	return once(task(arg))

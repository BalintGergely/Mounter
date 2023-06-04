from mounter.path import Path
from typing import Final, Set, Dict, List, final, TypeVar, Tuple
import asyncio

class Module:
	__key: Final
	def __init__(self,key):
		'''key is typically some unique identification of the Module.'''
		self.__key = tuple(key)

	@final
	def key(self):
		return self.__key
	
	def activate(self,context: 'Workspace'):
		'''
		Activate is called during module discovery. It signals that this object
		will be representing the module identified by it's key.
		This method should register all dependent modules.
		'''
		pass

	def run(self,context: 'Workspace'):
		'''
		In the execution phase, run() is invoked for all modules in dependency order.
		That is, all dependencies have been ran before this is invoked.
		If the module needs to do cleanup after dependents have run, use context.run(),
		clean up afterwards.
		'''
		pass

	@classmethod
	@final
	def manifest(cls) -> 'Module':
		return cls()

class Asyncio(Module):
	'''
	Module that sets up an asyncio event loop.
	Use the wait function in this module to wait for a task.
	'''
	def __init__(self):
		super().__init__(key = (__file__,"asyncio"))
	
	def run(self,context):
		self.__loop = asyncio.new_event_loop()
		try:
			asyncio.set_event_loop(self.__loop)
			context.run()
		finally:
			try:
				self.__loop.run_until_complete(self.__loop.shutdown_asyncgens())
				self.__loop.run_until_complete(self.__loop.shutdown_default_executor())
			finally:
				asyncio.set_event_loop(None)
				self.__loop.close()

	def wait(self,task):
		return self.__loop.run_until_complete(task)

T = TypeVar("T", bound=Module)

class Workspace:
	'''
	A flow controller between a set of dependent 'Modules'.
	Workspace has two phases. Discovery and Execution phase.

	When created, the workspace is initially in discovery phase.
	Use the add() and use() functions to register modules.

	In Discovery phase, modules are added to the workspace, and are
	topologically sorted into a list, such that each module occurs
	before any of it's dependents.

	Execution phase begins when run() is invoked on the Workspace.
	No more modules may be added beyond this point.

	In Execution phase, run() is invoked for all modules in the order they
	appear in the list.
	'''
	__inactiveModules : Dict[Tuple,Module]
	__activeModules: Dict[Tuple,Module]
	__topology: List[Module]
	__topologyIndex: int

	def __init__(self):
		self.__activeModules = {}
		self.__inactiveModules = {}
		self.__topology = []
		self.__topologyIndex = 0
		pass
	
	def __getitem__(self,mod: T) -> T:
		'''Fetch a specific module.'''
		realMod = self.__activeModules[mod.manifest().key()]
		assert realMod is not None
		return realMod
	
	def run(self):
		'''Begin or continue executing modules.'''
		while self.__topologyIndex < len(self.__topology):
			module = self.__topology[self.__topologyIndex]
			self.__topologyIndex += 1
			module.run(self)
	
	def use(self,mod : Module):
		'''
		Register the specific module as a non-default implementation.
		The module is only activated when any of it's dependents are activated.
		'''
		key = mod.key()
		assert key not in self.__inactiveModules and key not in self.__activeModules, (
			"use() or add() was already invoked for this module!")
		self.__inactiveModules[key] = mod

		return mod
	
	def add(self,mod: T) -> T:
		'''
		Register and activate the specific module.
		The module instance is returned.
		'''
		assert self.__topologyIndex == 0, "Cannot add modules after run is called!"
		mod: Module = mod.manifest()
		assert isinstance(mod, Module), "Only subclasses of Module are accepted!"
		key = mod.key()
		if key in self.__activeModules:
			assert self.__activeModules[key] is not None, "Recursive call to add() with key "+str(key)+"!"
			return self.__activeModules[key]
		else:
			mod = self.__inactiveModules.pop(key, mod)
			self.__activeModules[key] = None
			mod.activate(self)
			self.__topology.append(mod)
			self.__activeModules[key] = mod
			return mod


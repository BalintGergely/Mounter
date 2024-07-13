
from typing import Any, Dict, List, final, Final, TypeVar, Type

class ModuleInitContext():
	def __init__(self, workspace : 'Workspace') -> None:
		self._workspace = workspace

class Module():
	key : Any

	def __new__(cls, contextArg = ...) -> None:
		if contextArg is ...:
			return cls
		else:
			return super().__new__(cls)

	def __init__(self, context : ModuleInitContext) -> None:
		self.ws : Final[Workspace] = context._workspace

	def __init_subclass__(cls) -> None:
		if "key" not in cls.__dict__:
			setattr(cls,"key",cls)
		pass

	def _downstream(self):
		"""
		Call this to run downstream modules.
		"""
		self.ws.run()

	def run(self):
		"""
		In the execution phase, run() is invoked for all modules in dependency order.
		That is, all dependencies have been ran before this is invoked.
		"""
		pass

	@final
	@classmethod
	def manifest(cls):
		return cls

class AppendHook():
	def __init__(self,fnc,mod : Module) -> None:
		self.run = fnc
		self.parent = mod

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
	appear in the list. Modules may also opt to do a cleanup action after all their dependents have run.
	'''
	T = TypeVar("T",bound = Module)

	def __init__(self):
		self.__activeModules : Dict[object,Module] = {}
		self.__inactiveModules : Dict[object,type] = {}
		self.__topology : List[Module] = []
		self.__topologyIndex : int = 0
		self.__runningModule = None
		pass
	
	def __getitem__(self,mod : Type[T]) -> T:
		'''Fetch a specific module.'''
		mod = mod.manifest()
		realMod = self.__activeModules[mod.key]
		assert realMod is not None
		return realMod
	
	def __contains__(self,mod : Type[T]) -> bool:
		mod = mod.manifest()
		if mod.key in self.__activeModules:
			modInstance = self.__activeModules[mod.key]
			if isinstance(modInstance,mod):
				return True
		return False

	def getCurrentExecutingModule(self):
		'''Returns the module which is currently running. Used for debugging.'''
		return self.__runningModule
	
	def run(self):
		'''Begin or continue executing modules.'''
		mc = self.__runningModule
		while self.__topologyIndex < len(self.__topology):
			module = self.__topology[self.__topologyIndex]
			self.__topologyIndex += 1
			if isinstance(module,AppendHook):
				self.__runningModule = module.parent
			else:
				self.__runningModule = module
			module.run()
		self.__runningModule = mc
	
	def use(self,mod : Type[T]):
		'''
		Register the specific module as a non-default implementation.
		The module is only activated when any of it's dependents are activated.
		'''
		mod = mod.manifest()
		assert issubclass(mod, Module), "Only subclasses of Module are accepted!"
		key = mod.key
		assert key is not None, "key may not be None"
		assert key not in self.__inactiveModules and key not in self.__activeModules, \
			"use() or add() was already invoked for this module!"
		self.__inactiveModules[key] = mod
		return mod
	
	def append(self, fnc):
		'''
		Append a custom function to be executed directly after the run invocation of currently active modules.
		Can be used both in the discovery and execution phase. The function receives no arguments.
		'''
		self.__topology.append(AppendHook(fnc, self.__runningModule))
	
	def add(self,mod : Type[T]) -> T:
		'''
		Register and activate the specific module.
		The module instance is returned.
		'''
		mod = mod.manifest()
		assert self.__topologyIndex == 0, "Cannot add modules after run is called!"
		assert issubclass(mod, Module), "Only subclasses of Module are accepted!"
		key = mod.key
		assert key is not None, "key may not be None"
		if key in self.__activeModules:
			assert self.__activeModules[key] is not None, f"Recursive call to add() with key {key}!"
			return self.__activeModules[key]
		else:
			mod = self.__inactiveModules.pop(key, mod)
			self.__activeModules[key] = None
			ins = mod(ModuleInitContext(self))
			self.__topology.append(ins)
			self.__activeModules[key] = ins
			return ins
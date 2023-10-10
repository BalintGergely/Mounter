from mounter.path import Path
from typing import Iterator, Set, Dict, Awaitable, Iterable, Callable, Any, NoReturn, Tuple, Final, List
from itertools import chain, tee
import asyncio
import subprocess
import mounter.workspace as workspace
from typing import final

class Operation:
	'''
	A runnable async operation that has a set of states required for it to run and a set of states it causes.

	States are identified by hashable objects. They are not tracked and are not required to have a way to test their presence.
	'''
	def getResultStates(self) -> Iterable:
		'''States this operation will meet.'''
		return ()

	def getRequiredStates(self) -> Iterable:
		'''States this operation requires.'''
		return ()

	async def runAsync(self):
		return self.run()
	
	def run(self):
		assert False, "run() method has not been overridden for type"+type(self)
		
	def opHash(self):
		assert False, "opHash() method has not been overridden for type"+type(self)

@final
class Gate(Operation):
	'''
	Class to attach metadata to operations.
	Can be used to add required or result states in cases where those cannot be normally deduced.
	Gates can also be marked goals when performing operations selectively.
	'''
	__required: Set
	__result: Set
	__internal: Operation
	__goal: bool
	__name: str
	def __init__(self,
	    	internal: Operation = None,
	    	requires: Iterable = ...,
			produces: Iterable = ...,
			goal = ...,
			name = ...,
			base = ...):
		if isinstance(base,Gate):
			if internal is ...:
				internal = base.__internal
			if requires is ...:
				requires = base.__required
			if produces is ...:
				produces = base.__result
			if goal is ...:
				goal = base.__goal
			if name is ...:
				name = base.__name
		self.__required = set() if requires is ... else set(requires)
		self.__result = set() if produces is ... else set(produces)
		self.__goal = False if goal is ... else goal
		self.__name = None if name is ... else name
		if internal is not None:
			self.__required.update(internal.getRequiredStates())
			self.__result.update(internal.getResultStates())
			if isinstance(internal, Gate):
				self.__internal = internal.__internal
			else:
				self.__internal = internal
		else:
			self.__internal = None
		
		self.context = None
	
	def getRequiredStates(self) -> Iterable:
		yield from self.__required
	
	def getResultStates(self) -> Iterable:
		yield from self.__result
	
	def isGoal(self) -> bool:
		'''Whether running this gate is a goal.'''
		return self.__goal
	
	def hasInternal(self) -> bool:
		'''Whether this gate does anything when ran.'''
		return self.__internal is not None
	
	def hasRequired(self) -> bool:
		'''Whether this gate has any required states.'''
		return bool(self.__required)
	
	def hasResult(self) -> bool:
		'''Whether this gate has any result states.'''
		return bool(self.__result)
	
	def nonGoal(self) -> 'Gate':
		return Gate(goal=False,base=self)

	async def runAsync(self):
		if self.__internal is not None:
			return await self.__internal.runAsync()
	
	def run(self):
		if self.__internal is not None:
			return self.__internal.run()
	
	def opHash(self):
		if self.__internal is None:
			return ""
		else:
			return self.__internal.opHash()
	
	def __str__(self):
		definition = ""
		if self.__name is not None:
			definition = f"\"{self.__name}\": "
		if self.__internal is not None:
			return definition + str(self.__internal).replace("\n","\n"+definition)
		definition = "\n".join([definition+"Output: "+str(f) for f in self.__result]+[definition+"Input: "+str(f) for f in self.__required])
		#if self.__internal is not None:
		#	definition = definition + "\n" + str(self.__internal)
		if self.__goal:
			definition = "Goal: "+definition.replace("\n","\nGoal: ")
		return definition

	def __hash__(self) -> int:
		h = hash(self.__goal) ^ hash(self.__internal) ^ hash(self.__name)
		for req in self.__required:
			h = h ^ hash(req)
		for res in self.__result:
			h = h ^ res
		return h

class Silence(Operation):
	'''
	Silences an internal operation. Instead of running it, print(str(self)) will be called.
	'''
	__internal: Operation
	def __init__(self, internal: Operation):
		self.__internal = internal
	
	def getRequiredStates(self) -> Iterable[Path]:
		return self.__internal.getRequiredStates()
	
	def getResultStates(self) -> Iterable[Path]:
		return self.__internal.getResultStates()
	
	def run(self):
		print(str(self))
	
	def opHash(self):
		return None # Since we do not actually perform the operation, op hash is None.
	
	def __str__(self):
		return "Silenced: "+str(self.__internal).replace("\n","\nSilenced: ")

class Copy(Operation):
	'''
	Operation to copy a single file to single target file.
	Requirement is the source file existing, result is the target file existing.
	'''
	__source: Path
	__target: Path

	def __init__(self, source: Path, target: Path):
		self.__source = source
		self.__target = target
	
	def getRequiredStates(self):
		yield self.__source
	
	def getResultStates(self):
		yield self.__target
	
	def run(self):
		self.__source.opCopyTo(self.__target)
	
	def opHash(self):
		return f"cat {self.__source} > {self.__target}"
	
	def __str__(self):
		return "Copy: "+str(self.__source)+"\nTo: "+str(self.__target)

class CreateDirectories(Operation):
	'''
	Operation that creates an empty directory.
	'''
	__directories: Tuple[Path]
	__empty: bool

	def __init__(self, *directories: Path, empty: bool = False):
		self.__directories = tuple(directories)
		self.__empty = empty
	
	def run(self):
		for d in self.__directories:
			if d.isDirectory():
				if self.__empty:
					for file in d.getPostorder(includeSelf=False):
						file.opDelete()
			else:
				d.opCreateDirectories()
	
	def getResultStates(self) -> Iterable:
		return self.__directories

	def opHash(self):
		if self.__empty: # Whatever works.
			return ";".join(f"mkdir -p --clean {p}" for p in sorted(self.__directories))
		else:
			return ";".join(f"mkdir -p {p}" for p in sorted(self.__directories))

	def __str__(self) -> str:
		if self.__empty:
			return "\n".join(f"Create empty directory: {p}" for p in self.__directories)
		else:
			return "\n".join(f"Create directory: {p}" for p in self.__directories)

class Cluster(Operation):
	'''
	An operation that performs all of a given set of operations
	ensuring correct ordering and parallelism.
	'''
	__operations: List[Operation]
	__resultStates: Dict
	def __init__(self,all: Iterable[Operation]) -> None:
		self.__operations = tuple(all)
		self.__resultStates = {}
		self.__requiredStates = set()
		for (index,op) in enumerate(self.__operations):
			self.__requiredStates.update(op.getRequiredStates())
			for state in op.getResultStates():
				assert state not in self.__resultStates, f"Duplicate state {state}"
				self.__resultStates[state] = index
		
		self.__requiredStates.difference_update(self.__resultStates.keys())
	
	def getRequiredStates(self) -> Iterable[Path]:
		yield from self.__requiredStates
	
	def getResultStates(self) -> Iterable[Path]:
		yield from self.__resultStates.keys()

	def __atopo(self,index,ops):
		'''
		Returns the task for the specific operation index.
		If the task has not been generated yet, generates it.
		'''
		o = ops[index]
		if isinstance(o,tuple):
			return o[0]
		
		op : Operation = o
		
		toWait: List[Operation] = []
		for state in op.getRequiredStates():
			if state in self.__resultStates:
				toWait.append(self.__atopo(self.__resultStates[state],ops))
		
		async def runWhenReady():
			for t in toWait:
				await t
			return await op.runAsync()

		task = asyncio.create_task(runWhenReady())
		ops[index] = (task,)

		return task
	
	async def runAsync(self):
		ops = list(self.__operations)
		ts = [self.__atopo(x,ops) for x in range(len(self.__operations))]
		for t in ts:
			await t
	
	def __stopo(self,index,ops) -> Iterator[Operation]:
		'''
		Yields all requisite operations, finally the operation at index.
		'''
		op = ops[index]
		if op is not None:
			ops[index] = None
			for state in op.getRequiredStates():
				if state in self.__resultStates:
					yield from self.__stopo(self.__resultStates[state],ops)
			yield op
	
	def run(self):
		ops = list(self.__operations)
		for x in range(len(ops)):
			for t in self.__stopo(x,ops):
				t.run()
	
	def opHash(self):
		l = [f"[{h.opHash()}]" for h in self.__operations]
		l.sort()
		return "\n".join(l)

	def __str__(self) -> str:
		return "\n".join(str(h) for h in self.__operations)

class Sequence(Operation):
	'''
	An operation that performs the given list of operations in sequence.
	'''
	def __init__(self,all: Iterable[Operation]) -> None:
		self.__operations = tuple(all)
		self.__resultStates = set()
		self.__requiredStates = set()
		for op in self.__operations:
			for required in op.getRequiredStates():
				if required not in self.__resultStates:
					self.__requiredStates.add(required)
			for result in op.getResultStates():
				assert result not in self.__requiredStates, "Bad order of operations in a Sequence!"
				assert result not in self.__resultStates, f"Duplicate state {result}"
				self.__resultStates.add(result)
	
	def getRequiredStates(self) -> Iterable[Path]:
		yield from self.__requiredStates
	
	def getResultStates(self) -> Iterable[Path]:
		yield from self.__resultStates
	
	async def runAsync(self):
		for op in self.__operations:
			await op.runAsync()
	
	def run(self):
		for op in self.__operations:
			op.run()
	
	def opHash(self):
		return "\n".join(f"[{h.opHash()}]" for h in self.__operations)

	def __str__(self) -> str:
		return "\n".join(str(h) for h in self.__operations)

class Command(Operation):
	'''Operation to run an asyncio command.'''
	__command: Tuple[str]
	def __init__(self, *args: str):
		self.__command = tuple(str(k) for k in args)

	async def runAsync(self):
		proc = await asyncio.create_subprocess_shell(subprocess.list2cmdline(self.__command),
		stdout=asyncio.subprocess.PIPE,
		stderr=asyncio.subprocess.PIPE)
		
		stdout, stderr = await proc.communicate()

		print(stdout.decode(), end="")
		print(stderr.decode(), end="")

		if proc.returncode != 0:
			raise Exception(subprocess.list2cmdline(self.__command))
	
	def run(self):
		proc = subprocess.Popen(self.__command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		stdout, stderr = proc.communicate()
		print(stdout.decode(), end="")
		print(stderr.decode(), end="")
		if proc.returncode != 0:
			raise Exception(subprocess.list2cmdline(self.__command))
	
	def opHash(self):
		return " ".join(f"\"{k}\"" for k in self.__command)

	def __str__(self):
		return "Execute: "+(subprocess.list2cmdline(self.__command))

class Module(workspace.Module):

	def __init__(self,useAsync = False):
		super().__init__(key = __file__)
		self.__useAsync = useAsync
	
	def activate(self, context):
		if self.__useAsync:
			context.add(workspace.Asyncio)
		self.__ops: List[Operation] = []
		self.__res: Dict[str,workspace.Module] = {}
	
	def filterOperations(self, operations: List[Operation], context):
		return operations
	
	def _runOperation(self, context, operation):
		if self.__useAsync:
			context[workspace.Asyncio].wait(operation.runAsync())
		else:
			operation.run()
	
	def run(self, context):

		self.__context = context

		context.run()

		end = Cluster(self.filterOperations(self.__ops, context))
		f = list(end.getRequiredStates())
		assert len(f) == 0, "\n".join(["Required states have not been accounted for:"]+[str(k) for k in f])
		self._runOperation(context, end)

		self.__ops = None
	
	def add(self,op: Operation):
		self.__ops.append(op)
		for r in op.getResultStates():
			# It is best to catch an error like this here and now.
			if r in self.__res:
				prevMod = self.__res[r]
				nowMod = self.__context.getCurrentExecutingModule()
				if prevMod is nowMod:
					raise Exception(f"Duplicate state {r}, registered by module {prevMod}")
				else:
					raise Exception(f"Duplicate state {r}, registered by module {prevMod} then {nowMod}")
			self.__res[r] = self.__context.getCurrentExecutingModule()

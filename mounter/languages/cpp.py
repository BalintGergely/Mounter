
from mounter.path import Path, RelativePath
from typing import List, Dict, Set, Tuple, Iterable
from shutil import which
import itertools
import mounter.workspace as workspace
import mounter.operation as operation
from mounter.operation import Gate, Command, Module as OperationModule
from mounter.workspace import Workspace

CPP_IGNORE = 0
CPP_SOURCE = 1
CPP_SOURCE_MAIN = 2
CPP_STATICALLY_LINKED = 3
CPP_DYNAMICALLY_LINKED = 4

class CppGroup:
	"""
	This is a group of files associated with a CppProject.
	The group implementation is supplied by the cpp compiler module.
	"""
	def addInput(self,p: Path, project: bool = False, private: bool = False, main: bool | str = ..., extension = ...):
		"""
		Add inputs to the group.
		'project': Also add all subpaths in the directory.
		'private': Do not allow other projects to #include this directory.
		'main': Indicates whether files being added are main files.
		Can also specify a string to pass the name of the main file.
		'extension': Pretend that the file has the specific extension.
		"""
		raise Exception("Not implemented")

	def addOutput(self,p: Path,executable: bool = True):
		"""
		Add an output to the group.
		"""
		raise Exception("Not implemented")

	def addGoal(self, goalState, private: bool = False):
		"""
		Registers a state to be added to all goals that will be generated based on this group.
		'private': If true, the state is only for executables from this group and not in depenedent groups.
		"""
		raise Exception("Not implemented")

	def use(self,c: 'CppGroup'):
		pass

class CppModule(workspace.Module):
	def __init__(self):
		super().__init__(key = __file__)
	
	def activate(self, context: Workspace):
		raise Exception("This is an abstract module. You need to register the implementation to use it!")

	def newGroup(self) -> CppGroup:
		raise Exception("Not implemented!")

class SupportsCppGroup():
	def cppGroup():
		raise Exception("Not implemented")

class CppProject(workspace.Module,SupportsCppGroup):
	def __init__(self, projectFile, *dependencies):
		super().__init__(projectFile)
		self._path = Path(projectFile).getParent()
		self.__dependencies = tuple(dependencies)
		self._main = ...
	
	def activate(self, context: workspace.Workspace):
		context.add(CppModule)
		self.__dependencies = tuple(context.add(d) for d in self.__dependencies)
	
	def collectSources(self):
		"""All files that are supplied by the project. This is used in a supply gate."""
		return self._path.getPreorder()

	def fillGroup(self, group: CppGroup, context : workspace.Workspace = None):
		group.addInput(self._path, project = True, main = self._main)
	
	def cppGroup(self):
		return self._group
	
	def run(self, context : workspace.Workspace):
		cppmod : CppModule = context[CppModule]
		opmod : OperationModule = context[OperationModule]
		sources = self.collectSources()
		if sources is not None:
			opmod.add(Gate(produces=sources))
		self._group = cppmod.newGroup()
		self.fillGroup(self._group, context)
		for d in self.__dependencies:
			if isinstance(d,SupportsCppGroup):
				self._group.use(d.cppGroup())

class ClangGroup(CppGroup):

	def __init__(self, module : 'ClangModule', uid: int, rootDir: Path, binDir: Path):
		self.__uid = uid
		self.__clangModule = module
		self.rootDir = rootDir
		self.binDir = binDir
		self.objects: Set[Path] = set()                     # Set of object files.
		self.dependencies: List[(ClangGroup,bool)] = list() # List of group dependencies.
		self.units: Set[Path] = set()                       # Set of source files to compile.
		self.includes: Dict[Path,bool] = dict()             # Include paths.
		self.staticLibraries: Set[Path] = set()             # Static libraries.
		self.dynamicLibraries: Dict[Path,Path] = dict()     # Dynamic libraries
		self.goals: Dict[object,bool] = dict()              # Additional goals. (For use with resource files.)
		self.outputs: Dict[object,bool] = dict()            # Output files of this group.
		self.arguments: Dict[str,bool] = dict()             # Set of additional command line arguments for compiling
		self.extensions: Dict[str,int] = None
	
	def disableWarning(self,warning):
		self.arguments[f"-Wno-{warning}"] = False
	
	def addInput(self,p: Path, project: bool = False, private: bool = False, main: str | bool = ..., extension = ...):		

		def handleSingleFile(x : Path):
			ext = extension
			if ext == ...:
				ext = x.getExtension()
			fileKind = None
			if isinstance(extension,dict):
				fileKind = extension[ext.lower()]
			else:
				fileKind = {
					"cpp": CPP_SOURCE,
					"lib" : CPP_STATICALLY_LINKED,
					"dll" : CPP_DYNAMICALLY_LINKED}.get(ext.lower(),CPP_IGNORE)
			if fileKind == CPP_SOURCE or fileKind == CPP_SOURCE_MAIN:
				isMainFile = False
				if isinstance(main,bool):
					isMainFile = main
				elif isinstance(main,str):
					isMainFile = main in x.getName()
				elif fileKind == CPP_SOURCE_MAIN:
					isMainFile = True
				if isMainFile:
					g = self.__clangModule.newGroup()
					g.dependencies.append((self,True))
					g.units.add(x)
					g.addOutput(x.relativeToParent().moveTo(self.binDir).withExtension("exe"))
				else:
					self.units.add(x)
				return True
			if fileKind == CPP_DYNAMICALLY_LINKED:
				self.dynamicLibraries[x] = x.relativeToParent().moveTo(self.binDir)
				return True
			if fileKind == CPP_STATICALLY_LINKED:
				self.staticLibraries.add(x)
				return True
			return False
	
		if p.isDirectory():
			if project:
				anyAdded = False
				for l in p.getLeaves():
					added = handleSingleFile(l)
					anyAdded = added or anyAdded
				assert anyAdded, "Specified a project directory, but no files were added."
			self.includes[p] = not bool(private)
		else:
			assert handleSingleFile(p), "File type is not recognisable."

	
	def addOutput(self, p: Path, executable: bool = True):
		self.outputs[p] = executable
	
	def addGoal(self, state, private: bool = False):
		"""
		Registers an additional state into the CppGroup.
		The state will be added to all goals that will be generated based on this group.
		"""
		if state in self.goals:
			private = not self.goals[state]
		self.goals[state] = not bool(private)
	
	def topo(self,visited : Set[int]):
		if self.__uid in visited:
			return
		visited.add(self.__uid)
		for (k,v) in self.dependencies:
			yield from k.topo(visited)
		yield self
	
	def getUID(self):
		return self.__uid

	def use(self,c):
		self.dependencies.append((c,False))

def sortKey(o):
	if isinstance(o,tuple):
		return tuple(str(v) for v in o)
	else:
		return (o,)

def makeCommand(procName: str,cmd: list):
	cmd = list(cmd)
	cmd.sort(key = sortKey)
	res = [procName]
	for o in cmd:
		if isinstance(o,tuple):
			res.extend(o)
		else:
			res.append(o)
	return Command(*res)

class ClangModule(CppModule):
	def __init__(self, root = Path(""), obj = Path("obj/bin"), src = Path("obj/cpp"), bin = Path("bin")):
		super().__init__()
		self.groups: List[ClangGroup] = []
		self.root = root
		self.obj = obj
		self.bin = bin
		self.src = src
		self.preprocess = True
		self.assemble = False
		self.debug = False
		self.useLLVM = None
		self.optimalize = False
		self.additionalArguments: Set[str] = set()
	
	def newGroup(self):
		c = ClangGroup(self, len(self.groups), self.root, self.bin)
		self.groups.append(c)
		return c
	
	def activate(self, context):
		context.add(operation)

	def run(self, context):
		context.run()
		opmod = context[operation]
		for op in self.makeOps():
			opmod.add(op)
	
	def makeOps(self):
		dynamicLibraries = set()

		useLLVM = self.useLLVM

		if useLLVM is None:
			if which("lld") != None:
				useLLVM = True
			else:
				useLLVM = False
		
		visited = set()
		topology : List[ClangGroup] = []

		for group in self.groups:
			for g in group.topo(visited):
				topology.append(g)
		
		for group in topology:
			for (k,nested) in group.dependencies:
				group.includes.update((i,p) for (i,p) in k.includes.items() if (p or nested))
				group.staticLibraries.update(k.staticLibraries)
				group.dynamicLibraries.update(k.dynamicLibraries)
				for (s,p) in k.goals.items():
					if p or nested:
						group.goals[s] = group.goals.get(s,False) or p
				for (s,p) in k.arguments.items():
					if p or nested:
						group.arguments[s] = group.arguments.get(s,False) or p
				group.objects.update(k.objects)

			compileArgs = list(self.additionalArguments)
			
			if useLLVM:
				compileArgs.append("-fuse-ld=lld")

			if(self.debug):
				compileArgs.append("-g")
				compileArgs.append("-O0")
			
			if(self.optimalize):
				compileArgs.append("-O3")

			# Generate commands to compile each object file.
			
			for inputPath in group.units:
				inputRelative = None
				preprocessPath = None
				objectPath = None

				if self.root.isSubpath(inputPath):
					inputRelative = inputPath.relativeTo(self.root)
				else:
					inputRelative = inputPath.relativeToParent()
				
				preprocessPath = inputRelative.moveTo(self.src).withExtension("cpp")

				extension = None

				if self.assemble:
					if useLLVM:
						extension = "ll"
					else:
						extension = "asm"
				else:
					if useLLVM:
						extension = "bc"
					else:
						extension = "o"

				objectPath = inputRelative.moveTo(self.obj).withExtension(extension)
				
				cmd = [inputPath] + compileArgs + list(group.arguments)
				req = set(group.includes)
				req.add(inputPath)
				for i in group.includes:
					cmd.append(("--include-directory",i))
					req.add(i)

				if self.preprocess:
					cmd.append("--preprocess")
					cmd.append(("-o",preprocessPath))
					yield Gate(requires=req,produces=[preprocessPath],internal=makeCommand("clang++",cmd))
					req = [preprocessPath]
					cmd = [preprocessPath] + compileArgs + list(group.arguments)
				
				if self.assemble:
					cmd.append("--assemble")
				else:
					cmd.append("--compile")
				
				if useLLVM:
					cmd.append("-emit-llvm")
				
				cmd.append(("-o",objectPath))
				yield Gate(requires=req,produces=[objectPath],internal=makeCommand("clang++",cmd))

				group.objects.add(objectPath)

			# Generate commands to compile each separate output file.
			
			for (a,b) in group.dynamicLibraries.items():
				dynamicLibraries.add((a,b))

			for (binary,isMain) in group.outputs.items():
				req = list(group.objects)
				runtime = set(group.dynamicLibraries.values())
				runtime.add(binary)
				cmd = [("-o",binary)] + compileArgs
				if not isMain:
					cmd.append("-shared")
				cmd.extend(group.objects)

				for lib in group.staticLibraries:
					cmd.append(("--for-linker",lib))
					req.append(lib)
				
				yield Gate(requires=req,produces=[binary],internal=makeCommand("clang++",cmd))

				runtime.update(group.goals.keys())

				yield Gate(requires=runtime,goal=True)

		for (a,b) in dynamicLibraries:
			yield operation.Copy(a,b)

def manifest():
	return CppModule()
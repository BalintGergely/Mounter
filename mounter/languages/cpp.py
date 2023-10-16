
from mounter.path import Path, RelativePath
from typing import List, Dict, Set, Tuple, Iterable
from shutil import which
import itertools
import mounter.workspace as workspace
import mounter.operation as operation
from mounter.operation import Gate, Command, Module as OperationModule
from mounter.workspace import Workspace

class CppGroup:
	"""
	This is a group of files associated with a CppProject.
	The group implementation is supplied by the cpp compiler module.
	"""
	def add(self,p: Path, project: bool = False, private: bool = False, extension = ...):
		"""
		'project': Also add all subfiles in the directory.
		'private': Do not allow other projects to #include this directory.
		'extension': Pretend that the file has the specific extension.
		"""
		pass

	def addGoal(self, goalState, private: bool = False):
		"""
		Registers a state to be added to all goals that will be generated based on this group.
		'private': If true, the state is only for executables from this group and not in depenedent groups.
		"""
		pass

	def use(self,c: 'CppGroup'):
		pass

class CppModule(workspace.Module):
	def __init__(self):
		super().__init__(key = __file__)
	
	def activate(self, context: Workspace):
		raise Exception("This is an abstract module. You need to register the implementation to use it!")

	def newGroup(self) -> CppGroup:
		raise Exception("Not implemented!")

class CppProject(workspace.Module):
	def __init__(self, projectFile, *dependencies):
		super().__init__(projectFile)
		self._path = Path(projectFile).getParent()
		self.__dependencies = tuple(dependencies)
	
	def activate(self, context: workspace.Workspace):
		context.add(CppModule)
		self.__dependencies = tuple(context.add(d) for d in self.__dependencies)
	
	def collectSources(self):
		"""All files that are supplied by the project. This is used in a supply gate."""
		return self._path.getPreorder()

	def fillGroup(self, group: CppGroup, context : workspace.Workspace = None):
		group.add(self._path, project = True)
	
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
			if isinstance(d,CppProject):
				self._group.use(d.cppGroup())

class ClangGroup(CppGroup):

	def __init__(self, uid: int, rootDir: Path, binDir: Path):
		self.__uid = uid
		self.rootDir = rootDir
		self.binDir = binDir
		self.objects: Set[Path] = set()
		self.dependencies: List[ClangGroup] = [] # List of group dependencies.
		self.units: Dict[Path,bool] = {} # Set of files to compile. Value indicates whether it is a main file.
		self.includes: Dict[Path,bool] = {} # Include paths. True if dependent groups inherit this.
		self.libraries: Dict[Path,Path] = {} # Library paths. Value is None for static libraries, otherwise where they need to be moved.
		self.goals: Dict[object,bool] = {} # Additional goals. Values if True if the goal is to be inherited by dependents.

	def add(self,p: Path, project: bool = False, private: bool = False, extension = ...):
		if extension == ...:
			extension = p.getExtension()
		if isinstance(p,Path) and p.isDirectory():
			if project:
				for l in p.getLeaves():
					if l.hasExtension("cpp"):
						self.units[l] = True
					elif l.hasExtension("hpp"):
						self.units[l] = False
			self.includes[p] = not bool(private)
		elif extension == "dll":
			self.libraries[p] = p.relativeToParent().moveTo(self.binDir)
		elif extension == "lib":
			self.libraries[p] = None
		elif extension == "cpp":
			self.units[p] = True
		elif extension == "hpp":
			self.units[p] = False
	
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
		for k in self.dependencies:
			yield from k.topo(visited)
		yield self

	def use(self,c):
		self.dependencies.append(c)

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
	
	def newGroup(self):
		c = ClangGroup(len(self.groups), self.root, self.bin)
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
		dlls = {}

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
			for k in group.dependencies:
				group.includes.update((i,p) for (i,p) in k.includes.items() if p)
				group.libraries.update(k.libraries)
				for (s,p) in k.goals.items():
					if p:
						group.goals[s] = True
				group.objects.update(k.objects)

			mains: List[Path] = []
			compileArgs = ["-std=c++20","-Wc++17-extensions"]
			
			if useLLVM:
				compileArgs.append("-fuse-ld=lld")

			if(self.debug):
				compileArgs.append("-g")
				compileArgs.append("-O0")
			
			if(self.optimalize):
				compileArgs.append("-O3")

			# Generate commands to compile each object file.
			
			for (inputPath,isMain) in group.units.items():
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
				
				cmd = ["clang++",inputPath] + compileArgs
				req = set(group.includes)
				req.add(inputPath)
				for i in group.includes:
					cmd.append("--include-directory")
					cmd.append(i)
					req.add(i)

				if self.preprocess:
					cmd.extend(["--preprocess","-o",preprocessPath])
					yield Gate(requires=req,produces=[preprocessPath],internal=Command(*cmd))
					req = [preprocessPath]
					cmd = ["clang++",preprocessPath] + compileArgs
				
				if self.assemble:
					cmd.append("--assemble")
				else:
					cmd.append("--compile")
				
				if useLLVM:
					cmd.append("-emit-llvm")
				
				cmd.extend(["-o",objectPath])
				yield Gate(requires=req,produces=[objectPath],internal=Command(*cmd))

				if isMain:
					mains.append(objectPath)
				else:
					group.objects.add(objectPath)

			# Generate commands to compile each separate main file.
			# Generate goals for each main file.

			for mainObject in mains:
				req = [mainObject]
				req.extend(group.objects)
				executable = mainObject.relativeToParent().moveTo(self.bin).withExtension("exe")
				runtime = set()
				runtime.add(executable)
				cmd = ["clang++",mainObject,"-o",executable] + compileArgs
				cmd.extend(group.objects)

				for (dll,dyn) in group.libraries.items():
					cmd.append("--for-linker")
					cmd.append(dll)
					req.append(dll)
					if dyn is not None:
						dlls[dll] = dyn
						runtime.add(dyn)
				
				yield Gate(requires=req,produces=[executable],internal=Command(*cmd))

				runtime.update(group.goals.keys())

				yield Gate(requires=runtime,goal=True)

		for (dll,dyn) in dlls.items():
			yield operation.Copy(dll,dyn)

def manifest():
	return CppModule()
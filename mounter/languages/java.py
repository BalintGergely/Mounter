from mounter.path import Path, RelativePath
from io import TextIOWrapper
from typing import List, Dict, Set
import mounter.workspace as workspace
import struct
import shutil
import zipfile
import itertools
from mounter.operation import Operation, Command, CreateDirectories, Module as OperationModule, Sequence
from mounter.languages.cpp import CppModule, CppProject, CppGroup, SupportsCppGroup
from mounter.operation import Gate, Command, Copy, Cluster
from mounter.workspace import Workspace

def manifest():
	return Module()

class JavaGroup:
	def __init__(self) -> None:
		# Arguments to pass to --module-path (Modules or directories containing modules)
		self.modulePaths : Set[Path] = set()
		# Arguments to pass to --class-path (Must be path to package hierarchy root.)
		self.classPaths : Set[Path] = set()
		# Arguments to pass to --source-path (Must be path to package hierarchy root.)
		self.sourcePaths : Set[Path] = set()
		# List of .java files to compile
		self.sourceFiles : Set[Path] = set()
		# Set of annotation processor source files and class names. Either may be None.
		self.processors : Set[(Path,str)] = set()
		# List of class resource files
		self.resourceFiles : Set[Path] = set()
		pass
	
	def addModulePath(self,p: Path):
		self.modulePaths.add(p)
	
	def addClassPath(self,p: Path):
		self.classPaths.add(p)
	
	def addSourcePath(self,p: Path):
		self.sourcePaths.add(p)
	
	def addSourceFiles(self,p: Path):
		for f in p.getLeaves():
			if f.hasExtension("java"):
				self.sourceFiles.add(f)
	
	def addSourceFile(self,p: Path):
		self.sourceFiles.add(p)
	
	def addProcessor(self,source: Path = None,name: str = None):
		self.processors.add((source,name))
	
	def addResourceFiles(self,p: Path,*extensions):
		if p.isFile():
			assert len(extensions) == 0, "Extensions may not be specified when adding a single file."
			self.resourceFiles.add(p)
		else:
			for f in p.getLeaves():
				if f.getExtension() in extensions:
					self.resourceFiles.add(f)
	
	def use(self,c: 'JavaGroup'):
		pass

class JavaProject(workspace.Module):
	def __init__(self, projectFile: str, *dependencies):
		super().__init__((projectFile,))
		self._path = Path(projectFile).getParent()
		self.__dependencies = tuple(dependencies)
	
	def activate(self, context: Workspace):
		context.add(Module)
		self.__dependencies = tuple(context.add(d) for d in self.__dependencies)
	
	def collectSources(self):
		return self._path.getPreorder()
	
	def fillGroup(self, group : JavaGroup):
		group.addSourcePath(self._path)
		group.addSourceFiles(self._path)
	
	def run(self, context: Workspace):
		javam : Module = context[Module]
		opmod : OperationModule = context[OperationModule]
		
		sources = self.collectSources()
		if sources is not None:
			opmod.add(Gate(produces=sources))

		group = javam.newGroup()

		self.fillGroup(group)

class Module(workspace.Module):
	def __init__(self, root = Path(""), obj = Path("obj/java"), include = Path("obj/java/CppInclude"),bin = Path("bin")):
		super().__init__(key = __file__)
		self._groups: List[JavaGroup] = []
		self._root = root
		self._obj = obj
		self._bin = bin
		self._include = include
		self.debug = False
		self.reflect = True
	
	def activate(self, context: Workspace):
		context.add(OperationModule)
		self.__theGroup = JavaGroup()
	
	def newGroup(self):
		return self.__theGroup

	def getInclude(self):
		return self._include

	def run(self, context):
		context.run()
		opmod: OperationModule = context[OperationModule]

		group = self.__theGroup

		commandBase = ["javac"]
		commandBase.extend(["-encoding","UTF-8"])
		if self.debug:
			commandBase.append("-g")
		else:
			commandBase.append("-g:none")
		
		if self.reflect:
			commandBase.append("-parameters")

		generatedSourcePath = self._obj.subpath("src")
		generatedClassPath = self._obj.subpath("bin")

		commandBase.extend(["-d",generatedClassPath])

		requiredStates = set(group.sourceFiles)
		
		for (a,b) in itertools.combinations(group.sourcePaths,2):
			assert (not a.isSubpath(b)) and (not b.isSubpath(a)),"Overlapping source paths not allowed!"

		for md in sorted(group.modulePaths):
			commandBase.extend(["--module-path",md])
			requiredStates.add(md)
		
		for cp in sorted(group.classPaths):
			commandBase.extend(["--class-path",cp])
			requiredStates.add(cp)
		
		for sp in sorted(group.sourcePaths):
			commandBase.extend(["--source-path",sp])
			requiredStates.add(sp)

		twoPass = len(group.processors) > 0

		opSequence = list()

		opSequence.append(CreateDirectories(generatedClassPath,generatedSourcePath,self._include,empty = True))

		if twoPass:
			firstPass = list(commandBase)
			secondPass = list(commandBase)

			firstPass.append("-proc:none")
			
			secondPass.extend(["-h",self._include])
			secondPass.extend(["-s",generatedSourcePath])

			firstPassPaths = set()
			for (f,c) in sorted(group.processors):
				if f is not None:
					firstPassPaths.add(f)
					requiredStates.add(f)
				if c is not None:
					secondPass.extend(["-processor",c])
			
			secondPass.extend(sorted(group.sourceFiles))
			firstPass.extend(sorted(firstPassPaths))

			opSequence.append(Command(*firstPass))
			opSequence.append(Command(*secondPass))
		else:
			secondPass = list(commandBase)
			secondPass.append("-proc:none")
			secondPass.extend(["-h",self._include])
			secondPass.extend(sorted(group.sourceFiles))
			opSequence.append(Command(*secondPass))

		resourceOps = []

		for re in sorted(group.resourceFiles):
			rt = None
			for sp in group.sourcePaths:
				if re.isSubpath(sp):
					rt = re.relativeTo(sp)
					break
			
			assert rt is not None, f"Resource {re} must be in a source directory."

			target = rt.moveTo(generatedClassPath)

			resourceOps.append(Copy(re,target))
		
		if len(resourceOps) != 0:
			opSequence.append(Cluster(resourceOps))

		opSequence[-1] = Gate(requires=requiredStates,internal=opSequence[-1])

		opmod.add(Sequence(opSequence))

		opmod.add(Gate(requires=[generatedClassPath],goal=True))
		
class CppNativeBinding(workspace.Module,SupportsCppGroup):
	"""
	This provides a CppGroup corresponding to the native headers generated by the java compiler.
	A CppProject may add this as a dependency.
	"""
	def __init__(self):
		super().__init__(key = (__file__,"nativecpp"))
	
	def activate(self, context: Workspace):
		context.add(OperationModule)
		context.add(CppModule)
		context.add(Module)

	def run(self, context: Workspace):
		javamod : Module = context[Module]
		cppmod : CppModule = context[CppModule]
		opmod : OperationModule = context[OperationModule]
		javaexe = Path(shutil.which("javac"))
		javainclude = javaexe.getParent().getParent().subpath("include")

		self._group = cppmod.newGroup()
		self._group.addInput(javamod.getInclude())
		self._group.addInput(javainclude)

		opmod.add(Gate(produces=[javainclude]))
	
	def cppGroup(self):
		return self._group

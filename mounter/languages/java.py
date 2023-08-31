from mounter.path import Path
from io import TextIOWrapper
from typing import List, Dict, Set
import mounter.workspace as workspace
import struct
import shutil
import zipfile
from mounter.operation import Operation, Command, CreateDirectories, Module as OperationModule, Sequence
from mounter.languages.cpp import CppModule, CppProject, CppGroup
from mounter.operation import Gate, Command
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
        
	def use(self,c: 'JavaGroup'):
		pass

class JavaProject(workspace.Module):
	def __init__(self, projectFile: str, *dependencies):
		super().__init__((projectFile,))
		self.path = Path(projectFile).getParent()
		self.__dependencies = tuple(dependencies)
	
	def activate(self, context: Workspace):
		context.add(Module)
		self.__dependencies = tuple(context.add(d) for d in self.__dependencies)
	
	def collectSources(self):
		return self.path.getPreorder()
	
	def run(self, context: Workspace):
		javam : Module = context[Module]
		opmod : OperationModule = context[OperationModule]
		
		sources = self.collectSources()
		if sources is not None:
			opmod.add(Gate(produces=sources))

		group = javam.newGroup()

		group.addSourceFiles(self.path)

class Module(workspace.Module):
	def __init__(self, root = Path(""), obj = Path("obj/java"), include = Path("obj/java/CppInclude"),bin = Path("bin")):
		super().__init__(key = __file__)
		self.groups: List[JavaGroup] = []
		self.root = root
		self.obj = obj
		self.bin = bin
		self.include = include
		self.debug = False
		self.reflect = True
	
	def activate(self, context: Workspace):
		context.add(OperationModule)
		self.__theGroup = JavaGroup()
	
	def newGroup(self):
		return self.__theGroup

	def run(self, context):
		context.run()
		opmod: OperationModule = context[OperationModule]
		for op in self.makeCompileOperation():
			opmod.add(op)
	
	def makeCompileOperation(self):
		group = self.__theGroup

		commandBase = ["javac"]
		commandBase.extend(["-encoding","UTF-8"])
		if self.debug:
			commandBase.append("-g")
		else:
			commandBase.append("-g:none")
		
		if self.reflect:
			commandBase.append("-parameters")

		generatedSourcePath = self.obj.subpath("src")
		generatedClassPath = self.obj.subpath("bin")

		commandBase.extend(["-d",generatedClassPath])

		requiredStates = set(group.sourceFiles)

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

		opSequence.append(CreateDirectories(generatedClassPath,generatedSourcePath,self.include,empty = True))

		if twoPass:
			firstPass = list(commandBase)
			secondPass = list(commandBase)

			firstPass.append("-proc:none")
			
			secondPass.extend(["-h",self.include])
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
			secondPass.extend(["-h",self.include])
			secondPass.extend(sorted(group.sourceFiles))
			opSequence.append(Command(*secondPass))

		opSequence[-1] = Gate(requires=requiredStates,internal=opSequence[-1])
		
		yield Sequence(opSequence)

		yield Gate(requires=(generatedClassPath,),goal=True)

class Native(workspace.Module):
	def __init__(self, key):
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
		self._group.add(javamod.include)
		self._group.add(javainclude)

		opmod.add(Gate(produces=[javainclude]))
	
	def cppGroup(self):
		return self._group

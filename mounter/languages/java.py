from mounter.path import Path
from io import TextIOWrapper
from typing import List, Dict, Set
import mounter.workspace as workspace
import shutil
from mounter.operation import Operation, Command, Module as OperationModule, uniqueState
from mounter.languages.cpp import CppModule, CppProject, CppGroup
from mounter.operation import Gate, Command
from mounter.workspace import Workspace

class JavaGroup:
	def __init__(self) -> None:
		# Arguments to pass to --module-path (Modules or directories containing modules)
		self.modulePaths : Set[Path]
		# Arguments to pass to --class-path (Must be path to package hierarchy root.)
		self.classPaths : Set[Path] = set()
		# Arguments to pass to --source-path (Must be path to package hierarchy root.)
		self.sourcePaths : Set[Path] = set()
		# Arguments to pass to --source-module-path (Module sources)
		self.sourceModulePaths : Set[Path] = set()
		# List of .java files to compile
		self.sourceFiles : Set[Path] = set()
		# Set of annotation processor files and class names. Class name may be null.
		self.processors : Set[(Path,str)] = set()
		pass
    
	def add(self,p: Path, project: bool = False):
		if p.isDirectory():
			if project:
				for file in p.getLeaves():
					if file.hasExtension("java"):
						self.sourceFiles.add(file)
			else:
				self.classPaths.add(p)
		if p.isFile():
			if p.hasExtension("java"):
				self.sourceFiles.add(file)
        
	def use(self,c: 'JavaGroup'):
		pass

class JavaModule(workspace.Module):
	def __init__(self, root = Path(""), obj = Path("obj/java"), include = Path("obj/java/CppInclude"),bin = Path("bin")):
		super().__init__(key = __file__)
		self.groups: List[JavaGroup] = []
		self.root = root
		self.obj = obj
		self.bin = bin
		self.include = include
		self.__theGroup = JavaGroup()
	
	def newGroup(self):
		return self.__theGroup

	def run(self, context):
		context.run()
		opmod: OperationModule = context[OperationModule]
		for op in self.makeOps():
			opmod.add(op)
	
	def makeOps(self):
		group = self.__theGroup

		commandBase = ["java"]
		commandBase.extend(["-encoding","UTF-8"])
		commandBase.extend(["-g","-parameters"])

		generatedSourcePath = self.obj.child("src")
		generatedClassPath = self.obj.child("bin")

		commandBase.extend(["-s",generatedSourcePath])
		commandBase.extend(["-d",generatedClassPath])

		requiredStates = set()

		for md in group.modulePaths:
			commandBase.extend(["--module-path",md])
			requiredStates.add(md)
		
		for cp in group.classPaths:
			commandBase.extend(["--class-path",cp])
			requiredStates.add(cp)
		
		for sp in group.sourcePaths:
			commandBase.extend(["--source-path",sp])
			requiredStates.add(sp)
		
		for smp in group.sourceModulePaths:
			commandBase.extend(["--source-module-path",smp])
			requiredStates.add(smp)

		twoPass = len(group.processors) > 0

		commands = list()

		finalResultStates = [generatedClassPath,generatedSourcePath,self.include]

		if twoPass:
			firstPass = list(commandBase)
			secondPass = list(commandBase)

			firstPass.append("-proc:none")
			for (f,c) in group.processors:
				if c is not None:
					secondPass.extend(["-processor",c])
			
			secondPass.append("-implicit:none")
			secondPass.extend(["-h",self.include])

			firstPassPaths = set()
			for (f,c) in group.processors:
				firstPassPaths.add(f)
				if c is not None:
					secondPass.extend(["-processor",c])
			
			secondPass.extend(group.sourceFiles)
			firstPass.extend(firstPassPaths)

			commands.append(Command(*firstPass))
			commands.append(Command(*secondPass))
		else:
			secondPass = list(commandBase)
			secondPass.append("-proc:none")
			secondPass.append("-implicit:none")
			secondPass.extend(["-h",self.include])
			secondPass.extend(group.sourceFiles)
			commands.append(Command(*secondPass))

		intermediateState = None

		for (index,command) in enumerate(commands):
			isFirst = index == 0
			isLast = index == len(commands)-1

			localRequiredStates = list(requiredStates)
			
			if not isFirst:
				localRequiredStates.append(intermediateState)

			localResultStates = None

			if isLast:
				localResultStates = finalResultStates
			else:
				intermediateState = uniqueState("java compile step")
				localResultStates = [intermediateState]

			yield Gate(requires=localRequiredStates,produces=localResultStates,internal=command)

class JavaNatives(workspace.Module):
	def __init__(self, key):
		super().__init__(key = (__file__,"nativecpp"))
	
	def activate(self, context: Workspace):
		context.add(OperationModule)
		context.add(CppModule)
		context.add(JavaModule)

	def run(self, context: Workspace):
		javamod : JavaModule = context[JavaModule]
		cppmod : CppModule = context[CppModule]
		opmod : OperationModule = context[OperationModule]
		javaexe = Path(shutil.which("javac"))
		javainclude = javaexe.getParent().getParent().child("include")

		group = cppmod.newGroup()
		group.add(javamod.include)
		group.add(javainclude)

		opmod.add(Gate(produces=[javainclude]))


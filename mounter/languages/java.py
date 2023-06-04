from mounter.path import Path
from typing import List, Dict, Set
import mounter.workspace as workspace
import mounter.operation as operation
from mounter.languages.cpp import CppModule, CppProject, CppGroup
from mounter.operation import Gate, Command
from mounter.workspace import Workspace

class JavaGroup:
	def __init__(self) -> None:
		# List of modules (module hierarchy)
		self.modules : Set[Path]
		# List of class paths (package hierarchy)
		self.classPaths : Set[Path] = set()
		# List of .java files to compile
		self.sourceFiles : Dict[Path] = set()
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
	
	def newGroup(self):
		c = JavaGroup()
		self.groups.append(c)
		return c

	def run(self, context):
		context.run()
		classPaths = set()
		sourceFiles = set()
		for g in self.groups:
			classPaths.update(g.classPaths)
			sourceFiles.update(g.sourceFiles)
		
		commandBase = ["java"]
		commandBase.extend(["-encoding","UTF-8"])
		commandBase.extend(["-parameters"])

		generatedSourcePath = self.obj.child("src")
		generatedClassPath = self.obj.child("bin")

		commandBase.extend(["-s",generatedSourcePath])
		commandBase.extend(["-d",generatedClassPath])

		commandPassOne = list(commandBase)
		commandPassTwo = list(commandBase)

class JavaNatives(CppProject):
	def __init__(self):
		super().__init__(key = (__file__,"nativecpp"))
	
	def activate(self, context: Workspace):
		context.add(JavaModule)
		context.add(CppModule)
	
	def fillGroup(self, group: CppGroup):
		# Just the header directory as an include.
		group.add(self.__include, project = False, generated = True)
	
	def run(self, context: Workspace):
		java : JavaModule = context[JavaModule]
		self.__include = java.include
		super().run(context)


from mounter.path import Path
from typing import List, Dict, Set
import mounter.workspace as workspace
import mounter.operation as operation
from mounter.operation import Gate, Command

class JavaGroup:
	def __init__(self) -> None:
		# List of directories containing .class files or jars, zips.
		self.classFiles : Set[Path] = set()
		# List of .java files or directories, jars, zips containing .java files.
		self.sourceFiles : Set[Path] = set()
		self.dependencies : List[JavaGroup] = []
		pass
    
	def add(self,p: Path, source: bool = ...):
		if source is ...:
			if p.isDirectory():
				source = True
			elif p.hasExtension("class"):
				source = False
			elif p.hasExtension("jar"):
				source = False
			elif p.hasExtension("java"):
				source = True
			elif p.hasExtension("zip"):
				source = True
			
		assert isinstance(source,bool), "Unable to determine the Java file kind!"

		if source:
			self.sourceFiles.add(p)
		else:
			self.classFiles.add(p)
        
	def use(self,c: 'JavaGroup'):
		pass # Do nothing. All are treated equal... for now.

class JavaModule(workspace.Module):
	def __init__(self):
		super().__init__(__file__)
		self.groups: List[JavaGroup] = []
	
	def newGroup(self):
		c = JavaGroup(self.root, self.obj, self.bin)
		self.groups.append(c)
		return c
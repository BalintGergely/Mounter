from mounter.path import Path
from typing import List, Dict, Set
import mounter.workspace as workspace
import mounter.operation as operation
from mounter.operation import Gate, Command

class CppGroup:
	"""
	This is a group of cpp files associated with a CppProject.
	The group implementation is supplied by the cpp compiler module.
	"""
	def add(self,p: Path, project: bool = False, private: bool = False):
		"""
		'project': Also add all subfiles in the directory.
		'private': Do not allow other projects to #include this directory.
		"""
		pass
	
	def use(self,c: 'CppGroup'):
		pass

class CppModule(workspace.Module):
	def __init__(self):
		super().__init__(__file__)

	def newGroup(self):
		raise Exception("Not implemented!")

class CppProject(workspace.Module):
	def __init__(self, projectFile, *dependencies):
		super().__init__(projectFile)
		self.path = Path(projectFile).getParent()
		self.__dependencies = tuple(dependencies)
	
	def activate(self, context: workspace.Workspace):
		context.add(CppModule)
		self.__dependencies = tuple(context.add(d) for d in self.__dependencies)
	
	def fillGroup(self, group: CppGroup):
		group.add(self.path, "project")
	
	def group(self):
		return self._group
	
	def run(self, context):
		cppmod = context[CppModule]
		self._group = cppmod.newGroup()
		self.fillGroup(self._group)
		for d in self.__dependencies:
			self._group.use(d.group())

class ClangGroup(CppGroup):

	def __init__(self, rootDir: Path, objDir: Path, binDir):
		self.rootDir = rootDir
		self.objDir = objDir
		self.binDir = binDir
		self.dependencies: List[CppGroup] = [] # List of group dependencies.
		self.provisions: Set[Path] = set() # Set of input (source) files and directories.
		self.units: Dict[Path,bool] = {} # Set of main files to compile. Value indicates whether it is a main file.
		self.includes: Dict[Path,bool] = {} # Include paths. True if dependent groups inherit this.
		self.libraries: Dict[Path,Path] = {} # Library paths. Value is None for static libraries, otherwise where they need to be moved.

	def add(self,p: Path, project: bool = False, private: bool = False):
		self.provisions.add(p)
		if p.isDirectory():
			if project:
				for l in p.getLeaves():
					self.provisions.add(l)
					if l.hasExtension("cpp"):
						self.units[l] = True
					elif l.hasExtension("hpp"):
						self.units[l] = False
			self.includes[p] = not bool(private)
		elif p.hasExtension("dll"):
			self.libraries[p] = p.relativeToParent().moveTo(self.binDir)
		elif p.hasExtension("lib"):
			self.libraries[p] = None
		elif p.hasExtension("cpp"):
			self.units[p] = True
		elif p.hasExtension("hpp"):
			self.units[p] = False
	
	def updateUse(self):
		for k in self.dependencies:
			k.updateUse()
			self.includes.update((i,p) for (i,p) in k.includes.items() if p)
			self.libraries.update(k.libraries)
	
	def use(self,c):
		self.dependencies.append(c)

class ClangModule(CppModule):
	def __init__(self, root = Path(""), obj = Path("obj"), bin = Path("bin")):
		super().__init__()
		self.groups: List[CppGroup] = []
		self.root = root
		self.obj = obj
		self.bin = bin
		self.preprocess = True
		self.assemble = False
		self.debug = False
		self.optimalize = False
	
	def newGroup(self):
		c = ClangGroup(self.root, self.obj, self.bin)
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
		for group in self.groups:

			yield operation.Gate(produces=group.provisions)

			group.updateUse()
			objects = []
			mains: List[Path] = []
			compileArgs = ["-std=c++20","-Wc++17-extensions"]

			if(self.debug):
				compileArgs.append("-g")
				compileArgs.append("-O0")
			
			if(self.optimalize):
				compileArgs.append("-O3")

			# Generate commands to compile each object file.
			
			for (inputPath,isMain) in group.units.items():
				outputBase = None
				preprocessPath = None
				objectPath = None

				if self.root.isChildPath(inputPath):
					outputBase = inputPath.relativeTo(self.root)
				else:
					outputBase = inputPath.relativeToParent()
				
				outputBase = outputBase.moveTo(self.obj)
				preprocessPath = outputBase.withExtension("cpp")
				objectPath = outputBase.withExtension("ll" if self.assemble else "o")
				
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
				cmd.extend(["-emit-llvm","-o",objectPath])
				yield Gate(requires=req,produces=[objectPath],internal=Command(*cmd))

				if isMain:
					mains.append(objectPath)
				else:
					objects.append(objectPath)

			# Generate commands to compile each separate main file.
			# Generate goals for each main file.

			for mainObject in mains:
				req = [mainObject]
				req.extend(objects)
				executable = mainObject.relativeToParent().moveTo(self.bin).withExtension("exe")
				runtime = [executable]
				cmd = ["clang++",mainObject,"-o",executable] + compileArgs
				cmd.extend(objects)

				for (dll,dyn) in group.libraries.items():
					cmd.append("--for-linker")
					cmd.append(dll)
					req.append(dll)
					if dyn is not None:
						dlls[dll] = dyn
						runtime.append(dyn)

				yield Gate(requires=req,produces=[executable],internal=Command(*cmd))

				yield Gate(requires=runtime,goal=True)

		for (dll,dyn) in dlls.items():
			yield operation.Copy(dll,dyn)

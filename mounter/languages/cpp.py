from mounter.path import Path
from typing import List, Dict, Set, Tuple
from shutil import which
import mounter.workspace as workspace
import mounter.operation as operation
from mounter.operation import Gate, Command, Module as OperationModule
from mounter.workspace import Workspace

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
		super().__init__(key = __file__)
	
	def activate(self, context: Workspace):
		raise Exception("This is an abstract module. You need to register the implementation to use it!")

	def newGroup(self) -> CppGroup:
		raise Exception("Not implemented!")

class CppProject(workspace.Module):
	def __init__(self, projectFile, *dependencies):
		super().__init__(projectFile)
		self.path = Path(projectFile).getParent()
		self.__dependencies = tuple(dependencies)
	
	def activate(self, context: workspace.Workspace):
		context.add(CppModule)
		self.__dependencies = tuple(context.add(d) for d in self.__dependencies)
	
	def collectSources(self):
		return self.path.getPreorder()

	def fillGroup(self, group: CppGroup):
		group.add(self.path, project = True)
	
	def cppGroup(self):
		return self._group
	
	def run(self, context):
		cppmod : CppModule = context[CppModule]
		opmod : OperationModule = context[OperationModule]
		sources = self.collectSources()
		if sources is not None:
			opmod.add(Gate(produces=sources))
		self._group = cppmod.newGroup()
		self.fillGroup(self._group)
		for d in self.__dependencies:
			self._group.use(d.cppGroup())

class ClangGroup(CppGroup):

	def __init__(self, rootDir: Path, binDir: Path):
		self.rootDir = rootDir
		self.binDir = binDir
		self.dependencies: List[ClangGroup] = [] # List of group dependencies.
		self.units: Dict[Path,bool] = {} # Set of files to compile. Value indicates whether it is a main file.
		self.includes: Dict[Path,bool] = {} # Include paths. True if dependent groups inherit this.
		self.libraries: Dict[Path,Path] = {} # Library paths. Value is None for static libraries, otherwise where they need to be moved.

	def add(self,p: Path, project: bool = False, private: bool = False):
		if p.isDirectory():
			if project:
				for l in p.getLeaves():
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
		c = ClangGroup(self.root, self.bin)
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

		for group in self.groups:

			group.updateUse()
			objects = []
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

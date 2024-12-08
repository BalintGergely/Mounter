
from subprocess import list2cmdline
import re
from typing import Coroutine, Set, List, FrozenSet, override, AsyncIterable, Awaitable
from mounter.operation.files import *
from mounter.path import *
from mounter.workspace import *
from mounter.delta import *
from mounter.persistence import *
from mounter.progress import *
from mounter.goal import *
from mounter.operation import *
from mounter.operation.completion import Instant, Task

CPP_STRING_LITERAL = re.compile((
	r"(?P<kind>L|u8|u|U)?" # Literal kind
	r"(?P<raw>R)?" # Whether it is raw
	r"\"(?(raw)(?P<delim>[^() \\]*)\()" # Prefix
	r"(?P<sequence>(?(raw).|(?:[^\\\"]|\\.))*?)" # Sequence
	r"(?(raw)\)(?P=delim))\"" # Suffix
),flags = re.DOTALL)

CPP_STRING_ESCAPE = re.compile((
	rb"\\("
	rb"(?P<control>[abfnrtv])"
	rb"|(?P<o>o{)?(?P<octal>(?(o)[0-7]+|[0-7]{1,3}))(?(o)})"
	rb"|x(?P<xopen>{)?(?P<hex>[0-9a-fA-F]+)(?(xopen)})"
	rb"|(?:(?P<uopen>u{)|(?P<u>u)|(?P<U>U))"
		rb"(?P<unicode>(?(uopen)[0-9a-fA-F]+)(?(u)[0-9a-fA-F]{4})(?(U)[0-9a-fA-F]{8}))"
		rb"(?(uopen)})"
	rb"|N{(?P<name>.+?)}"
	rb"|(?P<char>.))"
),flags = re.DOTALL)

CPP_LINE_MARKER = re.compile(fr"^#\s+(?P<line>\d+)\s+{CPP_STRING_LITERAL.pattern}", re.DOTALL | re.MULTILINE)

def cppEscapeSubstitution(m : re.Match[bytes]):
	# These DO come up in clang-generated preprocessed files...
	control = m["control"]
	octal = m["octal"]
	hex = m["hex"]
	unicode = m["unicode"]
	name = m["name"]
	char = m["char"]
	if control:
		return {
			b"a":b"\a",
			b"b":b"\b",
			b"f":b"\f",
			b"n":b"\n",
			b"r":b"\r",
			b"t":b"\t",
			b"v":b"\v"
			}[control]
	if octal:
		return bytes([int(octal,8)])
	if hex:
		return bytes([int(octal,16)])
	if unicode:
		return chr(int(hex,16)).encode()
	if name:
		return str(eval(f"\"\\N{{{name}}}\"")).encode()
	if char:
		return char
	raise Exception(f"Unrecognised escape sequence: {m.group()}")

def getLiteralContent(m : re.Match):
	sequence : str = m["sequence"]
	if m["raw"]:
		return sequence
	else:
		try:
			return CPP_STRING_ESCAPE.sub(cppEscapeSubstitution, sequence.encode()).decode()
		except Exception as exc:
			raise Exception(f"Error parsing {m.group()}: {exc.args}")

CPP_NOT_A_SOURCE = re.compile(r"<.*>")

def readIncludes(path : Path):
	includePaths = set()
	
	with path.open("r",encoding = "utf-8") as input:
		data = ""
		lastMatch = 0
		while True:
			nd = input.read(0x100000)
			if len(nd) == 0:
				break

			data = data[lastMatch:] + nd
			lastMatch = 0
			for lineMarkerMatch in CPP_LINE_MARKER.finditer(data):
				lastMatch = lineMarkerMatch.start()
				pathLiteral = getLiteralContent(lineMarkerMatch)
				if not CPP_NOT_A_SOURCE.fullmatch(pathLiteral):
					includePaths.add(Path(pathLiteral))
	
	return frozenset(includePaths)

class CppGroup():
	def getIncludes(self) -> FrozenSet[Path]:
		return Instant(frozenset())
	
	def getObjects(self) -> FrozenSet[Path]:
		return Instant(frozenset())
	
	def getStaticLibraries(self) -> FrozenSet[Path]:
		return Instant(frozenset())
	
	def getDynamicLibraries(self) -> FrozenSet[Path]:
		return Instant(frozenset())
	
	def getBinDirectory(self) -> Path:
		raise Exception("Not supported")
	
	def getCompileFlags(self) -> FrozenSet[str]:
		return Instant(frozenset())

	def onCompile(self, mainGroup : 'CppGroup'):
		"""
		Called simultaneously as the specified group is being compiled.
		Useful to perform indirectly related tasks.
		"""
		return Instant()
	
	def compile() -> Awaitable:
		raise Exception("Not a compilable CppGroup!")
	
	@final
	def __eq__(self, that):
		return self is that
	
	@final
	def __hash__(self):
		return id(self)

class InputCppGroup(CppGroup):
	def __init__(self,
			includes = (),
			objects = (),
			staticLibraries = (),
			dynamicLibraries = (),
			compileFlags = (),
			compileEventListeners = (),
			onLinkCallback = None) -> None:
		self.includes : Set[Path] = set(includes)
		self.objects : Set[Path] = set(objects)
		self.staticLibraries : Set[Path] = set(staticLibraries)
		self.dynamicLibraries : Set[Path] = set(dynamicLibraries)
		self.compileFlags : Set[str] = set(compileFlags)
		self.compileEventListeners : List[Callable] = list(compileEventListeners)
		self.onLinkCallback = onLinkCallback
	
	@override
	@once
	def getIncludes(self):
		return Instant(frozenset(self.includes))
	
	@override
	@once
	def getObjects(self):
		return Instant(frozenset(self.objects))
	
	@override
	@once
	def getStaticLibraries(self):
		return Instant(frozenset(self.staticLibraries))
	
	@override
	@once
	def getDynamicLibraries(self):
		return Instant(frozenset(self.dynamicLibraries))

	@override
	@once		
	def getCompileFlags(self) -> FrozenSet[str]:
		return Instant(frozenset(self.compileFlags))

	@override
	@once
	def onCompile(self, mainGroup : 'CppGroup'):
		return Gather(*[c(mainGroup) for c in self.compileEventListeners])

class AggregatorCppGroup(CppGroup):
	def __init__(self, dependencies : Dict[CppGroup,bool] = ()) -> None:
		self._dependencies : Dict[CppGroup,bool] = dict(dependencies)

	@op
	async def __lookupIncludes(self,allowPrivate) -> FrozenSet[Path]:
		tasks = [g.getIncludes() for (g,p) in self._dependencies.items() if (allowPrivate or p)]
		return frozenset([i for t in tasks for i in await t])

	@op
	async def __lookupObjects(self,allowPrivate) -> FrozenSet[Path]:
		tasks = [g.getObjects() for (g,p) in self._dependencies.items() if (allowPrivate or p)]
		return frozenset([o for t in tasks for o in await t])
	
	@op
	async def __lookupStaticLibraries(self,allowPrivate) -> FrozenSet[Path]:
		tasks = [g.getStaticLibraries() for (g,p) in self._dependencies.items() if (allowPrivate or p)]
		return frozenset([l for t in tasks for l in await t])
	
	@op
	async def __lookupDynamicLibraries(self,allowPrivate) -> FrozenSet[Path]:
		tasks = [g.getDynamicLibraries() for (g,p) in self._dependencies.items() if (allowPrivate or p)]
		return frozenset([l for t in tasks for l in await t])
	
	@op
	async def __lookupCompileFlags(self,allowPrivate) -> FrozenSet[str]:
		tasks = [g.getCompileFlags() for (g,p) in self._dependencies.items() if (allowPrivate or p)]
		return frozenset([l for t in tasks for l in await t])
	
	def getIncludes(self): return self.__lookupIncludes(False)
	def getObjects(self): return self.__lookupObjects(True)
	def getStaticLibraries(self): return self.__lookupStaticLibraries(False)
	def getDynamicLibraries(self): return self.__lookupDynamicLibraries(False)
	def getCompileFlags(self): return self.__lookupCompileFlags(False)
	def _getMyIncludes(self): return self.__lookupIncludes(True)
	def _getMyStaticLibraries(self): return self.__lookupStaticLibraries(True)
	def _getMyDynamicLibraries(self): return self.__lookupDynamicLibraries(True)
	def _getMyCompileFlags(self): return self.__lookupCompileFlags(True)

	@once
	def onCompile(self, mainGroup: CppGroup):
		return Gather(*[g.onCompile(mainGroup) for g in self._dependencies.keys()])

class ClangCppGroup(AggregatorCppGroup):
	def __init__(self,
			clangModule : 'ClangModule',
			dependencies : Dict[CppGroup,bool] = (),
			srcDirectory : Path = ...,
			binDirectory : Path = ...,
			objDirectory : Path = ...,
			rootDirectory : Path = ...,
			assemble : bool = False,
			debug : bool = False,
			useLLVM : bool = True,
			optimalize : bool = False,
			sources : AsyncIterable[Path] = None,
			outputName : str = None,
			) -> None:
		super().__init__(dependencies)
		self.cpp : Final[ClangModule] = clangModule
		self.ws : Final[Workspace] = clangModule.ws
		self.__rootDirectory = rootDirectory
		self.__objDirectory = objDirectory
		self.__binDirectory = binDirectory
		self.__srcDirectory = srcDirectory
		self.__assemble = assemble
		self.__debug = debug
		self.__useLLVM = useLLVM
		self.__sources = sources
		self.__optimalize = optimalize
		self.__outputName = outputName
	
	@op
	async def __getAdditionalArguments(self):
		flags = await self._getMyCompileFlags()
		args = set()
		for f in flags:
			if f.startswith("-std="):
				args.add(f)
			if f.startswith("-Wno"):
				args.add(f)
		return frozenset(args)
	
	@op
	async def getBinDirectory(self) -> Path:
		self.__binDirectory.opCreateDirectories()
		return self.__binDirectory
	
	async def __runCommandHandleResult(self, commandSeq, progressUnit):
		(rc, a, b) = await self.ws[AsyncOps].runCommand(commandSeq, progressUnit = progressUnit)
		if b != b'' or rc != 0:
			print(f"Error: {subprocess.list2cmdline(commandSeq)}")
			print(b.decode(),end="")
		if rc != 0:
			print(f"Process exited with code {rc}")
			raise Exception("Clang command fail")
		return a == b'' and b == b''

	@op
	async def __preprocess(self, sourceFile : Path) -> Path:
		"""
		Preprocess the specified source file.
		Returns after the operation is done, with the Path of the preprocessed file.
		"""
		with self.ws[Progress].register() as pu:
			outputFile = sourceFile \
				.relativeTo(self.__rootDirectory) \
				.moveTo(self.__srcDirectory) \
				.withExtension("cpp")
			includes,args,_ = await Gather(
				self._getMyIncludes(),
				self.__getAdditionalArguments(),
				self.ws[AsyncOps].redLight()
			)

			args = sorted(args)

			deltaChecker = self.ws[FileDeltaChecker]

			dependencyHash = []
			dependencyHash.append(Task(deltaChecker.query(sourceFile)))

			for i in sorted(includes):
				dependencyHash.append(Task(deltaChecker.query(PathSet(f"{i}/**/"))))
			
			dependencyHash = [await d for d in dependencyHash]
			
			data = self.ws[FileManagement].lock(outputFile, self)
			includeHash = data.get("includeHash",())
			
			cmd = ["clang++",sourceFile,"-CC","--preprocess","-o",outputFile]
			cmd.append("-finput-charset=UTF-8")
			cmd.extend(args)
			for i in includes:
				cmd.extend(["--include-directory",i])
			
			cmd = [str(c) for c in cmd]

			pu.setName(list2cmdline(cmd))

			if data.get("dependencyHash",None) != dependencyHash \
			or data.get("args",None) != args \
			or not data.get("stable",None) \
			or not all([await k for k in [Task(deltaChecker.test(v)) for v in includeHash]]) \
			or not outputFile.isPresent():
				data.clear()				
				data["args"] = args
				data["dependencyHash"] = dependencyHash
				st = False
				
				async def finalizeData():
					nonlocal outputFile
					nonlocal self
					nonlocal includes
					nonlocal data
					nonlocal st
					includeHash = []
					ipset = await self.ws[AsyncOps].callInBackground(functools.partial(readIncludes, outputFile))
					for path in ipset:
						if any(i.isSubpath(path) for i in includes):
							includeHash.append(Task(deltaChecker.query(path)))
					data["includeHash"] = [await k for k in includeHash]
					data["stable"] = st

				outputFile.getAncestor().opCreateDirectories()
				st = await self.__runCommandHandleResult(cmd, pu)
				self.ws[AsyncOps].completeLater(finalizeData())
			else:
				pu.setUpToDate()
			return outputFile
	
	@op
	async def __compile(self, sourceFile) -> Path:
		"""
		Compiles the specified source file. (This includes preprocessing)
		Returns after the operation is done, with the Path of the object file.
		"""
		with self.ws[Progress].register() as pu:
			preFile,args,_ = await Gather(
				self.__preprocess(sourceFile),
				self.__getAdditionalArguments(),
				self.ws[AsyncOps].redLight()
			)
			
			args = sorted(args)

			extension = None
			deltaChecker = self.ws[FileDeltaChecker]
			if self.__assemble:
				if self.__useLLVM:
					extension = "ll"
				else:
					extension = "s"
			else:
				if self.__useLLVM:
					extension = "bc"
				else:
					extension = "o"
			
			flags = ""
			if self.__optimalize:
				flags = flags + "o"
			
			if self.__debug:
				flags = flags + "d"
			
			if flags != "":
				extension = f"{flags}.{extension}"
			
			outputFile = preFile.relativeTo(self.__srcDirectory) \
				.moveTo(self.__objDirectory) \
				.withExtension(extension)

			dependencyHash = [await deltaChecker.query(preFile)]

			cmd = ["clang++",preFile,"-o",outputFile]
			cmd.append("-finput-charset=UTF-8")
			cmd.extend(args)
			if self.__assemble:
				cmd.append("--assemble")
			else:
				cmd.append("--compile")
			if self.__useLLVM:
				cmd.append("-emit-llvm")
			if self.__debug:
				cmd.append("--debug")
			if self.__optimalize:
				cmd.append("-O3")
				if self.__useLLVM:
					cmd.append("-flto")
			
			cmd = [str(c) for c in cmd]

			pu.setName(list2cmdline(cmd))
			data = self.ws[FileManagement].lock(outputFile, self)

			if self.__debug and not data.get("debug",None) \
			or data.get("args",None) != args \
			or data.get("optimalize",False) != self.__optimalize \
			or not data.get("stable",None) \
			or data.get("dependencyHash",None) != dependencyHash \
			or not outputFile.isPresent():
				data.clear()
				outputFile.getAncestor().opCreateDirectories()
				stable = False
				try:
					stable = await self.__runCommandHandleResult(cmd, pu)
				finally:
					data["debug"] = self.__debug
					data["optimalize"] = self.__optimalize
					data["args"] = args
					data["stable"] = stable
					data["dependencyHash"] = dependencyHash
			else:
				pu.setUpToDate()
			return outputFile
	
	@override
	@op
	async def getObjects(self) -> FrozenSet[Path]:
		inherited = super().getObjects()
		if self.__sources is None:
			return await inherited
		if isinstance(self.__sources,AsyncIterable):
			tasks = [self.__compile(p) async for p in self.__sources]
		else:
			tasks = [self.__compile(p) for p in self.__sources]
		return (await inherited).union([await t for t in tasks])

	@op
	async def link(self) -> Path:
		"""
		Links the specific output file in this group.
		Returns it's path.
		"""
		with self.ws[Progress].register() as pu:
			deltaChecker = self.ws[FileDeltaChecker]

			(binDirectory,staticLibraries,allObjects,args,_) = await Gather(
				self.getBinDirectory(),
				self._getMyStaticLibraries(),
				self.getObjects(),
				self.__getAdditionalArguments(),
				self.ws[AsyncOps].redLight()
			)
			
			args = sorted(args)

			outputFile = binDirectory.subpath(self.__outputName)
			isMain = outputFile.hasExtension("exe")
			dependencyHash = []
			for o in sorted(allObjects):
				dependencyHash.append(Task(deltaChecker.query(o)))
			dependencyHash.append(Instant(None))
			for l in sorted(staticLibraries):
				dependencyHash.append(Task(deltaChecker.query(l)))
			
			dependencyHash = [await k for k in dependencyHash]
				
			cmd = ["clang++","-o",outputFile] + list(allObjects)
			cmd.append("-finput-charset=UTF-8")
			cmd.extend(args)
			if self.__debug:
				cmd.append("--debug")
			if self.__optimalize:
				cmd.append("-O3")
				if self.__useLLVM:
					cmd.append("-flto")
			if self.__useLLVM:
				cmd.append("-fuse-ld=lld")
			if not isMain:
				cmd.append("-shared")
			for lib in staticLibraries:
				cmd.append("--for-linker")
				cmd.append(lib)
			
			cmd = [str(c) for c in cmd]

			pu.setName(list2cmdline(cmd))
			data = self.ws[FileManagement].lock(outputFile, self)
			
			if data.get("debug",False) != self.__debug \
			or data.get("args",None) != args \
			or data.get("optimalize",False) != self.__optimalize \
			or not data.get("stable",False) \
			or data.get("dependencyHash",None) != dependencyHash \
			or not outputFile.isPresent():
				
				outputFile.getAncestor().opCreateDirectories()
				stable = False
				try:
					stable = await self.__runCommandHandleResult(cmd, pu)
				finally:
					data["debug"] = self.__debug
					data["optimalize"] = self.__optimalize
					data["args"] = args
					data["stable"] = stable
					data["dependencyHash"] = dependencyHash
			else:
				pu.setUpToDate()
			
			return outputFile
	
	@op
	async def copyDlls(self):
		(dllSet,binDirectory) = await Gather(self._getMyDynamicLibraries(),self.getBinDirectory())
		tasks = [self.ws[FileManagement].copyFile(l,l.relativeToAncestor().moveTo(binDirectory)) for l in dllSet]
		await self.ws[AsyncOps].redLight()
		await Gather(*tasks)
	
	@once
	def compile(self):
		return Gather(self.link(),self.copyDlls(),self.onCompile(self))

class CppModule(Module):
	def __init__(self, context) -> None:
		super().__init__(context)
		self.ws.add(FileManagement)
		self.ws.add(FileDeltaChecker)
		self.ws.add(Progress)
		self.ws.add(AsyncOps)
		self.rootDirectory = Path(".")
		self.objDirectory = self.rootDirectory.subpath("obj/cpp")
		self.binDirectory = self.rootDirectory.subpath("bin")
		self.srcDirectory = self.objDirectory
	
	def makeGroup(self,*,
			dependencies : Dict[CppGroup,bool] = ...,
			sources = ...,
			outputName = ...,
			**kwargs) -> AggregatorCppGroup:
		raise Exception("Not implemented")

def manifest():
	return CppModule

class ClangModule(CppModule):
	key = CppModule.key
	def __init__(self, context) -> None:
		super().__init__(context)
		self.assemble = False
		self.debug = False
		self.optimalize = False
	
	def makeGroup(self,**kwargs):
		def setDefault(key,value):
			nonlocal kwargs
			if kwargs.get(key,...) == ...:
				kwargs[key] = value
		setDefault("rootDirectory",self.rootDirectory)
		setDefault("objDirectory",self.objDirectory)
		setDefault("binDirectory",self.binDirectory)
		setDefault("srcDirectory",self.srcDirectory)
		setDefault("assemble",self.assemble)
		setDefault("debug",self.debug)
		setDefault("optimalize",self.optimalize)
		return ClangCppGroup(self,**kwargs)

class SupportsCppGroup():
	async def getCppGroup() -> CppGroup:
		raise Exception("Not implemented")

class CppProject(Module,SupportsCppGroup):
	def __init__(self, context, projectFile = None, *dependencies) -> None:
		super().__init__(context)
		self.ws.add(GoalTracker)
		self.ws.add(CppModule)
		self._dependencies = tuple(self.ws.add(d) for d in dependencies)
		if projectFile is not None:
			self._dir = Path(projectFile).getAncestor()
		else:
			self._dir = None
		self.group = InputCppGroup()
		self.group.compileEventListeners.append(self.onCompile)
		self.rootDirectory = ...
		self.privateGroup = InputCppGroup()
		self.compilationUnits : Set[Path] = set()
		self.mains : Set[Path | str] = set()
		self.__mainPaths : Set[Path] = set()
	
	def fillGroup(self):
		if self._dir is not None:
			self.group.includes.add(self._dir)
			for p in self._dir.getPreorder():
				if p not in self.__mainPaths and p.hasExtension("cpp","c"):
					self.compilationUnits.add(p)
	
	async def onCompile(self,mainGroup : CppGroup):
		pass
	
	@op
	async def getCppGroup(self):
		dependencies = {
			self.group : True,
			self.privateGroup : False
		}
		tasks : List[Awaitable[CppGroup]] = []
		for d in self._dependencies:
			if isinstance(d,SupportsCppGroup):
				tasks.append(d.getCppGroup())
		for t in tasks:
			dependencies[await t] = True
		return self.ws[CppModule].makeGroup(
			dependencies = dependencies,
			sources = self.compilationUnits,
			rootDirectory = self.rootDirectory)
	
	@op
	async def _compileExecutable(self,mainFile : Path,name : str):
		mainGroup = self.ws[CppModule].makeGroup(
			dependencies = [(await self.getCppGroup(),True)],
			sources = [mainFile],
			outputName = name)
		await mainGroup.compile()

	def run(self):
		for p in self.mains:
			if isinstance(p,str):
				p = self._dir.subpath(p)
			self.__mainPaths.add(p)
		self.fillGroup()
		for p in self.__mainPaths:
			name = p.withExtension("exe").getName()
			if self.ws[GoalTracker].defineThenQuery(name):
				self.ws[AsyncOps].completeLater(self._compileExecutable(p,name))

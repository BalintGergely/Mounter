import pathlib
import shutil
import re
import os
from typing import Hashable, Final, List, Tuple, Self, TextIO, BinaryIO, overload, Iterable

class Path(Hashable):
	"""
	Represents an absolute file Path.
	This always uses forward slash '/' for separators.
	"""
	__p: Final[pathlib.Path]
	def __new__(cls,path):
		if cls is Path and type(path) is Path:
			return path
		self = super().__new__(cls)
		if isinstance(path,Path):
			self.__p = path.__p
			self.__s = path.__s
		else:
			self.__p = pathlib.Path(path).absolute().resolve()
			self.__s = None
		return self
	
	def __hash__(self):
		return self.__p.__hash__()
	
	def _eqdc(self):
		return Path
	
	def __eq__(self,other):
		return isinstance(other,Path) and self._eqdc() == other._eqdc() and self.__p == other.__p
	
	def __lt__(self,other):
		return str(self) < str(other)

	def __gt__(self,other):
		return str(self) > str(other)
	
	def __str__(self):
		s = self.__s
		if s is None:
			s = self.__p.as_posix()
			while s.endswith("/"):
				s = s[:-1]
			self.__s = s
		return s
	
	def __repr__(self):
		return f"Path(\'{str(self)}\')"
	
	def hasExtension(self,*ext):
		e = self.getExtension()
		return any(x == e for x in ext)
	
	def resolve(self,subpath):
		return Path(self.__p.joinpath(subpath))
	
	def getExtension(self):
		n = self.__p.name
		k = n.rfind(".")
		if 0 <= k:
			return n[k+1:]
		else:
			return None
	
	def withExtension(self,ext):
		barePath = str(self)
		if "." in self.__p.name:
			barePath = barePath[0:barePath.rfind(".")]
		if ext is None:
			return Path(barePath)
		else:
			return Path(barePath+"."+ext)
	
	def isSubpath(self,other: 'Path') -> bool:
		subpath = str(other)
		mypath = str(self)
		return subpath.startswith(mypath) and (len(subpath) == len(mypath) or subpath[len(mypath)] == '/')
	
	def relativeTo(self,other: 'Path') -> 'RelativePath':
		return RelativePath(self,self.__p.relative_to(other.__p))
	
	def relativeToAncestor(self,steps : int = 1) -> 'RelativePath':
		return self.relativeTo(self.getAncestor(steps))

	def getAncestor(self,steps : int = 1) -> 'Path':
		"""
		Ancestor path the given number of layers up, if it exists. None if it does not.
		"""
		at = self.__p
		for _ in range(steps):
			pt = at.parent
			if pt == at:
				return None
			at = pt
		return Path(at)
	
	def getName(self) -> str:
		"""
		The last path element. The file name including extensions.
		"""
		return self.__p.name
	
	def subpath(self,child : str) -> 'RelativePath':
		return RelativePath(f"{self}/{child}",child)
	
	def opCreateFile(self):
		self.__p.touch()
	
	def opCreateDirectory(self):
		self.__p.mkdir()
	
	def opCreateDirectories(self):
		if not self.isDirectory():
			self.getAncestor().opCreateDirectories()
			self.opCreateDirectory()
	
	def opDeleteFile(self):
		self.__p.unlink()
	
	def opDeleteDirectory(self):
		self.__p.rmdir()
	
	def opDelete(self):
		if self.isDirectory():
			self.opDeleteDirectory()
		elif self.isFile():
			self.opDeleteFile()
	
	def opCopyTo(self,other : 'Path'):
		shutil.copy(src=str(self),dst=str(other))
	
	def isDirectory(self):
		return self.__p.is_dir()
	
	def isFile(self):
		return self.__p.is_file()
	
	def isPresent(self):
		return self.__p.exists()

	def getAncestors(self,includeSelf = False):
		"""
		A generator producing the Parents of this Path. Root first.
		"""
		p = self.getAncestor()
		if p is not None:
			yield from p.getAncestors(includeSelf=True)
		if includeSelf:
			yield self

	def getChildren(self,deterministic = False):
		"""
		A generator producing the direct children of this Path.
		"""
		if deterministic:
			return sorted(Path(p) for p in self.__p.iterdir())
		else:
			return (Path(p) for p in self.__p.iterdir())
	
	def getLeaves(self,deterministic = False):
		"""
		A generator producing all non-directory subpaths of this path.
		"""
		for f in self.getChildren(deterministic = deterministic):
			if f.isFile():
				yield f
			if f.isDirectory():
				yield from f.getLeaves(deterministic = deterministic)
	
	def getPreorder(self,includeSelf = True,deterministic = False):
		"""
		A generator producing all subpaths of this path in preorder.
		All paths are encountered before any of their subpaths.
		"""
		if includeSelf:
			yield self
		if self.isDirectory():
			for f in self.getChildren(deterministic = deterministic):
				yield from f.getPreorder(includeSelf = True,deterministic = deterministic)
	
	def getPostorder(self,includeSelf = True,deterministic = False):
		"""
		A generator producing all subpaths of this path in postorder.
		All paths are encountered after all their subpaths.
		"""
		if self.isDirectory():
			for f in self.getChildren(deterministic = deterministic):
				yield from f.getPostorder(includeSelf = True,deterministic = deterministic)
		if includeSelf:
			yield self
	
	def getBreadthFirst(self,includeSelf = True,deterministic = False):
		"""
		A generator producing all subpaths of this path in breadth first order.
		All paths are encountered before any path with more path elements.
		"""
		queue : List[Path] = list()

		if includeSelf:
			queue.append(self)
		elif self.isDirectory():
			queue.extend(self.getChildren(deterministic = deterministic))
		
		while len(queue) != 0:
			file = queue.pop(0)
			yield file
			if file.isDirectory():
				queue.extend(file.getChildren(deterministic = deterministic))
	
	def getModifiedTime(self):
		return os.path.getmtime(self.__p)

	def getContentLength(self):
		return os.path.getsize(self.__p)
	
	def open(self,flags,encoding : str = None) -> TextIO | BinaryIO:
		f = set()
		if encoding is None:
			f.add("b")
		else:
			assert isinstance(encoding,str), "Encoding argument must be str!"
			f.add("t")
		for k in flags:
			match k:
				case "r":
					f.add("r")
				case "w":
					f.add("w")
				case "a":
					f.add("a")
				case "x":
					f.add("x")
				case default:
					raise Exception("Unknown flag: "+k)
		return open(self.__p, "".join(k for k in f), encoding = encoding)
	
	def getIr(self):
		return self.__p

class RelativePath(Path):
	"""
	Still represents an absolute file path, but has a relative part for reference.
	"""
	def __new__(cls, path, subpath):
		self = super().__new__(cls, path)
		self._subpath = subpath
		return self

	def moveTo(self,target: Path) -> 'RelativePath':
		return RelativePath(target.getIr().joinpath(self._subpath),self._subpath)
	
	def relativeStr(self):
		return self._subpath.as_posix()

	def __repr__(self):
		return f"RelativePath({repr(str(self))},{repr(self._subpath)})"

__section = re.compile(
	 r"(?P<seplongstarepi>(?:/\*{2,})+$)"
	r"|(?P<seplongstarsep>(?:/\*{2,})+/)"
	r"|(?P<sep>/)"
	r"|(?P<ques>\?)"
	r"|(?P<longstarepi>\*{2,}$)"
	r"|(?P<longstar>\*{2,})"
	r"|(?P<star>\*)"
	r"|(?P<char>[-_ a-zA-Z0-9]+)"
	r"|(?P<misc>.)"
)

def _compilePattern(pathstr):
	preRegex = ""
	postRegex = ""
	groupCount = 0
	hitEndOnMatch = False
	for match in __section.finditer(pathstr):
		if match["sep"]:
			#  / : Match a path separator
			preRegex = preRegex + r"(/"
			postRegex = r")?" + postRegex
			groupCount += 1
		if match["ques"]:
			#  ? : Match any non-separator character.
			preRegex = preRegex + r"[^/]"
		if match["star"]:
			#  * : Match any sequence that does not contain a seperator.
			preRegex = preRegex + r"[^/]*?"
		if match["longstarepi"]:
			# Trailing ** : Match any possible suffix.
			preRegex = preRegex + r".*"
			hitEndOnMatch = True
		if match["longstar"]:
			#  ** : Match any sequence.
			preRegex = preRegex + r".*?("
			postRegex = r")?" + postRegex
			groupCount += 1
		if match["seplongstarsep"]:
			# /**/ special case: Also matches one separator.
			preRegex = preRegex + r"(?:(?=/).*?(/"
			postRegex = r")?)?" + postRegex
			groupCount += 1
			hitEndOnMatch = True
		if match["seplongstarepi"]:
			# /**  special case: Also matches empty.
			preRegex = preRegex + r"(?:/.*)?"
			hitEndOnMatch = True
		if match["char"]:
			# c : Match a specific character.
			preRegex = preRegex + match["char"]
		if match["misc"]:
			# m : Match a specific character.
			preRegex = preRegex + "\\" + match["misc"]
	return (re.compile(preRegex + postRegex),groupCount,hitEndOnMatch)

class PathSet(Hashable):
	"""
	Represents a set of Paths, defined by a pattern.
	All paths are valid patterns representing the singleton set containing that Path.
	"""

	# I had the idea to base this off of either regex or glob patterns,
	# but both use special characters that can occur in path names.
	
	__core = re.compile(r"(/?.*?)?((?=[^/]*[*?]).*?)?(/?)")

	__root : Path | None
	__pattern : str
	__compiled : re.Pattern
	__fullGroup : int
	__hitEndOnMatch : int
	__directoryOnly : bool
	__singleton : bool

	def __new__(cls, pattern : 'str | Path | PathSet'):
		if type(pattern) is PathSet:
			return pattern
		self = super().__new__(cls)
		if type(pattern) is Path:
			self.__root = pattern
			self.__pattern = str(pattern)
			self.__compiled = ()
			return
		
		assert type(pattern) is str

		(pattern,det,ndet,dir) = PathSet.__core.fullmatch(pattern).group(0,1,2,3)
		self.__compiled = ()
		self.__root = None
		self.__singleton = not bool(ndet)
		if det:
			self.__root = Path(det)
			if ndet:
				pattern = f"{self.__root}/{ndet}"
			else:
				pattern = f"{self.__root}"
		elif ndet:
			pattern = ndet
		(self.__compiled,self.__fullGroup,self.__hitEndOnMatch) = _compilePattern(pattern)
		self.__pattern = pattern + dir
		self.__directoryOnly = dir == "/"
		return self
	
	def __fullMatch(self,path : Path):
		m = self.__compiled.fullmatch(str(path))
		if m:
			lastGroup = m.group(self.__fullGroup)
			pm = (lastGroup is not None) and (not self.__directoryOnly or path.isDirectory())
			if lastGroup is None:
				he = True # Partial match.
			else:
				he = self.__hitEndOnMatch
		else:
			pm = False
			he = False
		return (pm,he)
	
	def findAll(self, path = ..., includePath = True, deterministic = False):
		"""
		Find all paths in the PathSet.
		"""

		if path is ...:
			assert self.__root is not None, "PathSet does not have a root."
			path = self.__root

		(pm,he) = self.__fullMatch(path)
		
		if pm and not he:
			for p in path.getPreorder(includeSelf = includePath, deterministic = deterministic):
				if not self.__directoryOnly or p.isDirectory():
					yield p
			return
		if pm and includePath:
			if not self.__directoryOnly or path.isDirectory():
				yield path
		if he and path.isDirectory():
			for p in path.getChildren(deterministic = deterministic):
				yield from self.findAll(path = p, includePath = True, deterministic = deterministic)

	def canFindAll(self):
		return self.__root is not None

	def isSingleton(self):
		"""
		True if this PathSet can only ever match one path.
		"""
		return self.__singleton

	def getRoot(self):
		assert self.__root is not None
		return self.__root

	def __iter__(self):
		return self.findAll(deterministic = True)
	
	def __contains__(self, path : Path):
		(pm,_) = self.__fullMatch(path)
		return pm

	def __hash__(self) -> int:
		return hash(self.__pattern)

	def __eq__(self, value: object) -> bool:
		return type(self) == type(value) and self.__pattern == value.__pattern
	
	def __repr__(self) -> str:
		return f"PathSet({repr(self.__pattern)})"

	def __str__(self) -> str:
		return self.__pattern

PathLike = Path | PathSet | str
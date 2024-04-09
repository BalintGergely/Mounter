import pathlib
import shutil
from io import TextIOWrapper
from typing import Hashable, Final, Generator, List

class Path(Hashable):
	'''
	Represents an absolute file Path.
	This always uses forward slash '/' for separators.
	'''
	__p: Final[pathlib.Path]
	def __init__(self,path):
		if isinstance(path,Path):
			self.__p = path.__p
		else:
			self.__p = pathlib.Path(path).absolute().resolve()
	
	def __hash__(self):
		return self.__p.__hash__()
	
	def __eq__(self,other):
		return isinstance(other,Path) and self.__p == other.__p
	
	def __lt__(self,other):
		return str(self) < str(other)

	def __gt__(self,other):
		return str(self) > str(other)
	
	def __str__(self):
		return self.__p.as_posix()
	
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
		barePath = self.__p.as_posix()
		if "." in self.__p.name:
			barePath = barePath[0:barePath.rfind(".")]
		if ext is None:
			return Path(barePath)
		else:
			return Path(barePath+"."+ext)
	
	def isSubpath(self,other: 'Path') -> bool:
		return str(self).startswith(str(other))
	
	def relativeTo(self,other: 'Path') -> 'RelativePath':
		return RelativePath(self,self.__p.relative_to(other.__p))
	
	def relativeToParent(self) -> 'RelativePath':
		return self.relativeTo(self.getParent())
	
	def getParent(self) -> 'Path':
		"""
		Parent path if exists. None if it does not.
		"""
		pt = self.__p.parent
		if pt == self.__p:
			return None
		return Path(pt)
	
	def getName(self) -> str:
		"""
		The last path element. The file name including extensions.
		"""
		return self.__p.name
	
	def subpath(self,child : str) -> 'RelativePath':
		return RelativePath(self.__p.as_posix()+"/"+child,child)
	
	def opCreateFile(self):
		self.__p.touch()
	
	def opCreateDirectory(self):
		self.__p.mkdir()
	
	def opCreateDirectories(self):
		if not self.isDirectory():
			self.getParent().opCreateDirectories()
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

	def getChildren(self):
		"""
		A generator producing the direct children of this Path.
		"""
		return (Path(p) for p in self.__p.iterdir())
	
	def getParents(self,includeSelf = False):
		"""
		A generator producing the Parents of this Path. Root first.
		"""
		p = self.getParent()
		if p is not None:
			yield from p.getParents(includeSelf=True)
		if includeSelf:
			yield self

	def getLeaves(self):
		"""
		A generator producing all non-directory subpaths of this path.
		"""
		for f in self.getChildren():
			if f.isFile():
				yield f
			if f.isDirectory():
				yield from f.getLeaves()
	
	def getPreorder(self,includeSelf = True):
		"""
		A generator producing all subpaths of this path in preorder.
		All paths are encountered before any of their subpaths.
		"""
		if includeSelf:
			yield self
		if self.isDirectory():
			for f in self.getChildren():
				yield from f.getPreorder()
	
	def getPostorder(self,includeSelf = True):
		"""
		A generator producing all subpaths of this path in postorder.
		All paths are encountered after all their subpaths.
		"""
		if self.isDirectory():
			for f in self.getChildren():
				yield from f.getPostorder()
		if includeSelf:
			yield self
	
	def getBreadthFirst(self,includeSelf = True):
		"""
		A generator producing all subpaths of this path in breadth first order.
		All paths are encountered before any path with more path elements.
		"""
		queue : List[Path] = list()

		if includeSelf:
			queue.append(self)
		elif self.isDirectory():
			queue.extend(self.getChildren())
		
		while len(queue) != 0:
			file = queue.pop(0)
			yield file
			if file.isDirectory():
				queue.extend(file.getChildren())
	
	def open(self,flags,encoding : str = None):
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
	'''
	Still represents an absolute file path, but has a relative part for reference.
	'''
	_subpath: Final[pathlib.Path] # both this and path point to the same file.
	def __init__(self,absolute,subpath):
		super().__init__(absolute)
		self._subpath = subpath

	def moveTo(self,target: Path) -> 'RelativePath':
		return RelativePath(target.getIr().joinpath(self._subpath),self._subpath)
	
	def relativeStr(self):
		return self._subpath.as_posix()
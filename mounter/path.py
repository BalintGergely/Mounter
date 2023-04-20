import pathlib
import shutil
from io import IO
from typing import Hashable, Final

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
	
	def hasExtension(self,ext):
		return self.getExtension() == ext
	
	def resolve(self,subpath):
		return Path(self.__p.joinpath(subpath))
	
	def getExtension(self):
		n = self.__p.name
		if "." in n:
			return n[n.rfind(".")+1:]
		else:
			return None
	
	def withExtension(self,ext):
		if "." in self.__p.name:
			pos = self.__p.as_posix()
			return Path(pos[0:pos.rfind(".")]+"."+ext)
		else:
			return Path(self.__p.as_posix()+"."+ext)
	
	def isChildPath(self,other: 'Path') -> bool:
		return str(self).startswith(str(other))
	
	def relativeTo(self,other: 'Path') -> 'RelativePath':
		return RelativePath(self,self.__p.relative_to(other.__p))
	
	def relativeToParent(self) -> 'RelativePath':
		return self.relativeTo(self.getParent())
	
	def getParent(self) -> 'Path':
		return Path(self.__p.parent)
	
	def getName(self) -> str:
		return self.__p.name
	
	def child(self,child):
		return Path(self.__p.as_posix()+"/"+child)
	
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
	
	def opCopyTo(self,other : 'Path'):
		shutil.copy(src=str(self),dst=str(other))
	
	def isDirectory(self):
		return self.__p.is_dir()
	
	def isFile(self):
		return self.__p.is_file()
	
	def isPresent(self):
		return self.__p.exists()

	def getChildren(self):
		return (Path(p) for p in self.__p.iterdir())
	
	def getLeaves(self):
		for f in self.getChildren():
			if f.isFile():
				yield f
			if f.isDirectory():
				yield from f.getLeaves()
	
	def open(self,flags) -> IO:
		f = set()
		enc = None
		for k in flags:
			match k:
				case "r":
					f.add("r")
				case "w":
					f.add("w")
				case "a":
					f.add("a")
				case "t":
					f.add("t")
					enc = "utf-8" # Never don't use utf8.
				case "b":
					f.add("b")
				case "x":
					f.add("x")
		return open(self.__p, "".join(k for k in f), encoding = enc)
	
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
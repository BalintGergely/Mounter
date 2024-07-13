
import hashlib
from typing import Dict, Type
from mounter.path import Path, PathSet, PathLike
from mounter.workspace import Module, Workspace
from mounter.persistence import Persistence

TYPE_NONE = b''
TYPE_FILE = b'f'
TYPE_DIR = b'd'

class PathCheckObject():
	__ignored = [
		PathSet("**/__pycache__/**")
	]

	def __init__(self, checker : 'FileDeltaChecker', path : PathLike, store : Dict[str,object]) -> None:
		self.checker = checker
		self.path = path
		self.store = store
		self.visited = False
		self.subpaths = ()
		self.version = None
		self.revisions : Dict[object,int] = dict()
		if "id" in self.store and "hash" in self.store:
			self.revisions[self.store["hash"]] = self.store["id"]
		
	def getPathType(self,hints = ()) -> bytes:
		if "deleted" in hints:
			return TYPE_NONE
		if self.path.isFile():
			return TYPE_FILE
		if self.path.isDirectory():
			return TYPE_DIR
		return TYPE_NONE

	def __refreshPathHash(self,hints = ()):
		self.subpaths = ()
		t = self.getPathType(hints)
		if t == TYPE_NONE:
			self.subpaths = ()
			self.store["time"] = None
			self.store["hash"] = None
			return
		md = self.store.get("time",...)
		fmd = self.path.getModifiedTime()
		if fmd == md:
			return
		dig = hashlib.md5()
		if t == TYPE_FILE:
			self.subpaths = ()
			with self.path.open("r") as input:
				while True:
					buf = input.read(0x1000)
					if len(buf) == 0:
						break
					dig.update(buf)
		if t == TYPE_DIR:
			def filterPath(path):
				if any(path in k for k in PathCheckObject.__ignored):
					return False
				return True
			self.subpaths = tuple(p for p in self.path.getChildren(deterministic = True) if filterPath(p))
			for p in self.subpaths:
				ch = self.checker.lookupChecker(p)
				ch.hintExists()
				dig.update(b'\0')
				dig.update(ch.getPathType())
				dig.update(b'\0')
				dig.update(p.getName().encode())
		self.store["hash"] = dig.hexdigest()
		self.store["time"] = fmd
	
	def __refreshSetHash(self, hints = ()):
		def filterPath(path):
			if any(path in k for k in PathCheckObject.__ignored):
				return False
			return True
		self.subpaths = tuple(p for p in self.path.findAll(deterministic = True) if filterPath(p))
		dig = hashlib.md5()
		for p in self.subpaths:
			ch = self.checker.lookupChecker(p)
			ch.hintExists()
			dig.update(b'\0')
			dig.update(str(p).encode())
			dig.update(b'\0')
			dig.update(ch.getHash().encode())
		self.store["hash"] = dig.hexdigest()

	def __refreshHash(self, hints = ()):
		oldSubpaths = self.subpaths
		if isinstance(self.path,PathSet):
			self.__refreshSetHash(hints)
		if isinstance(self.path,Path):
			self.__refreshPathHash(hints)
		if oldSubpaths is not self.subpaths:
			for p in (k for k in oldSubpaths if k not in self.subpaths):
				ch = self.checker.lookupChecker(p)
				ch.hintDeleted()

	def getHash(self) -> str:
		if not self.visited:
			self.__refreshHash()
			self.visited = True
		return self.store["hash"]

	def getVersion(self):
		if self.version is None:
			hash = self.getHash()
			vn = self.revisions.get(hash,None)
			if vn is None:
				self.revisions[hash] = vn = self.checker._newVersion()
				self.store["id"] = vn
			self.version = vn
		return self.version

	def clear(self):
		self.visited = False
		self.version = None
		if isinstance(self.path,PathSet):
			for p in self.subpaths:
				self.checker.lookupChecker(p).clear()
	
	def hintExists(self):
		"""
		Caller indicates that this path is known to exist.
		"""
		pass # Cannot do too many reasonable things here yet.

	def hintDeleted(self):
		self.visited = False
		self.version = None
		self.__refreshHash(("deleted",))

class FileDeltaChecker(Module):
	def __init__(self, context: Workspace) -> None:
		super().__init__(context)
		self.ws.add(Persistence)
	
	def _newVersion(self):
		c = self.__counter
		self.__counter += 1
		return c
	
	def run(self):
		self.__checkers : Dict[Path,PathCheckObject] = dict()
		self.__data = self.ws[Persistence].lookup(self)
		if "items" not in self.__data:
			self.__data["items"] = dict()
		self.__counter = self.__data.get("counter",0)
		self.__items = self.__data["items"]
		try:
			self._downstream()
		finally:
			self.__data["counter"] = self.__counter
			self.__checkers = None
			self.__items = None
			self.__data = None
	
	def lookupChecker(self, path : PathLike):
		pco = self.__checkers.get(path,None)
		if pco is None:
			key = str(path)
			store = self.__items.get(key, None)
			if store is None:
				self.__items[key] = store = dict()
			self.__checkers[path] = pco = PathCheckObject(self, path, store)
		return pco
	
	def __sanitizeQuery(self, path: PathLike):
		if isinstance(path,PathSet) and path.isSingleton():
			path = path.getRoot()
		return path
	
	def query(self, path: PathLike):
		path = self.__sanitizeQuery(path)
		ch = self.lookupChecker(path)
		return ch.getVersion()

	def clear(self, path: PathLike):
		path = self.__sanitizeQuery(path)
		ch = self.lookupChecker(path)
		ch.clear()

def manifest() -> Type[FileDeltaChecker]:
	return FileDeltaChecker


import hashlib
from typing import Dict, Type
from mounter.path import Path, PathSet, PathLike
from mounter.workspace import Module, Workspace
from mounter.persistence import Persistence

TYPE_NONE = b''
TYPE_FILE = b'f'
TYPE_DIR = b'd'

class PathCheckObject():
	def __init__(self, checker : 'FileDeltaChecker', path : PathLike, store : Dict[str,object]) -> None:
		self.checker = checker
		self.path = path
		self.store = store
		self.visited = False
		self.subpaths = ()
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
			self.subpaths = tuple(p for p in self.path.getChildren(deterministic = True) if self.checker.isFileRelevant(p))
			for p in self.subpaths:
				ch = self.checker.lookupCheckerByPath(p)
				ch.hintExists()
				dig.update(b'\0')
				dig.update(ch.getPathType())
				dig.update(b'\0')
				dig.update(p.getName().encode())
		fhs = dig.hexdigest()
		self.store["hash"] = fhs
		self.store["time"] = fmd
	
	def __refreshSetHash(self, hints = ()):
		self.subpaths = tuple(p for p in self.path.findAll(deterministic = True) if self.checker.isFileRelevant(p))
		dig = hashlib.md5()
		for p in self.subpaths:
			ch = self.checker.lookupCheckerByPath(p)
			ch.hintExists()
			dig.update(b'\0')
			dig.update(str(p).encode())
			dig.update(b'\0')
			dig.update(ch.getHash().encode())
		self.store["hash"] = dig.hexdigest()

	def __refreshHashAndVersion(self, hints = ()):
		oldSubpaths = self.subpaths
		if isinstance(self.path,PathSet):
			self.__refreshSetHash(hints)
		if isinstance(self.path,Path):
			self.__refreshPathHash(hints)
		
		vn = self.revisions.get(self.store["hash"], None)
		if vn is None:
			self.store.pop("id",None)
		else:
			self.store["id"] = vn
		
		if oldSubpaths is not self.subpaths:
			for p in (k for k in oldSubpaths if k not in self.subpaths):
				ch = self.checker.lookupCheckerByPath(p)
				ch.hintDeleted()

	def getHash(self) -> str:
		if not self.visited:
			self.__refreshHashAndVersion()
			self.visited = True
		return self.store["hash"]

	def getVersion(self):
		if not self.visited:
			self.__refreshHashAndVersion()
			self.visited = True
		vn = self.store.get("id",None)
		if vn is None: # Assign a new vn if absent.
			vn = self.checker._newVersion(self)
			self.revisions[self.store["hash"]] = vn
			self.store["id"] = vn
		return vn
	
	def testVersion(self, version : int):
		if not self.visited:
			self.__refreshHashAndVersion()
			self.visited = True
		return self.store.get("id",None) == version

	def clear(self):
		self.visited = False
		if isinstance(self.path,PathSet):
			for p in self.subpaths:
				self.checker.lookupCheckerByPath(p).clear()
	
	def hintExists(self):
		"""
		Caller indicates that this path is known to exist.
		"""
		pass # Cannot do too many reasonable things here yet.

	def hintDeleted(self):
		self.visited = False
		self.version = None
		self.__refreshHashAndVersion(("deleted",))

class FileDeltaChecker(Module):
	def __init__(self, context: Workspace) -> None:
		super().__init__(context)
		self.ws.add(Persistence)
	
	def _newVersion(self, checker : PathCheckObject):
		c = self.__counter
		self.__idmap[c] = checker
		self.__counter += 1
		return c
	
	__ignored = [
		PathSet("**/__pycache__/**"),
		PathSet("**/.git/**")
	]

	def isFileRelevant(self, path : Path):
		return not any(path in i for i in FileDeltaChecker.__ignored)
	
	def run(self):
		self.__checkers : Dict[Path,PathCheckObject] = dict()
		self.__data = self.ws[Persistence].lookup(self)
		if "items" not in self.__data:
			self.__data["items"] = dict()
		self.__counter = self.__data.get("counter",0)
		self.__items : Dict[str,Dict] = self.__data["items"]
		self.__idmap : Dict[int,PathCheckObject] = dict()
		for (pathstr,store) in self.__items.items():
			idv = store.get("id",None)
			if idv is not None:
				path = self.__sanitizeQuery(PathSet(pathstr))
				pco = PathCheckObject(self, path, store)
				self.__checkers[path] = pco
				self.__idmap[idv] = pco
		try:
			self._downstream()
		finally:
			for key in [key for (key,value) in self.__items.items() if len(value) == 0]:
				self.__items.pop(key)
			self.__data["counter"] = self.__counter
			self.__checkers = None
			self.__items = None
			self.__data = None
	
	def lookupCheckerByPath(self, path : PathLike):
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
		ch = self.lookupCheckerByPath(path)
		return ch.getVersion()
	
	def test(self, version : int):
		checker = self.__idmap.get(version,None)
		return checker is not None and checker.testVersion(version)

	def clear(self, path: PathLike):
		path = self.__sanitizeQuery(path)
		ch = self.lookupCheckerByPath(path)
		ch.clear()

def manifest() -> Type[FileDeltaChecker]:
	return FileDeltaChecker

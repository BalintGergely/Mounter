
import hashlib
from typing import Dict, Type
from mounter.path import Path, PathSet, PathLike
from mounter.workspace import Module, Workspace
from mounter.persistence import Persistence
from mounter.operation.completion import *
from mounter.operation import *

TYPE_NONE = b''
TYPE_FILE = b'f'
TYPE_DIR = b'd'

class PathCheckObject():
	def __init__(self, checker : 'FileDeltaChecker', path : PathLike, store : Dict[str,object]) -> None:
		self.checker = checker
		self.path = path
		self.store = store
		self.refreshTask = None
		self.subpaths = ()
		self.revisions : Dict[object,int] = dict()
		if "id" in self.store and "hash" in self.store:
			self.revisions[self.store["hash"]] = self.store["id"]
		
	def getPathType(self,flags = ()) -> bytes:
		if "deleted" in flags:
			return TYPE_NONE
		if self.path.isFile():
			return TYPE_FILE
		if self.path.isDirectory():
			return TYPE_DIR
		return TYPE_NONE
	
	async def __formatTypeAndName(self):
		return self.getPathType(flags = ("exists",)) + b'\0' + self.path.getName().encode()

	async def __formatPathAndHash(self):
		return str(self.path).encode() + b'\0' + (await self.getHash(flags = ("exists",))).encode()

	def __computeFileHash(self):
		dig = hashlib.md5()
		self.subpaths = ()
		with self.path.open("r") as input:
			while True:
				buf = input.read(0x100000)
				if len(buf) == 0:
					break
				dig.update(buf)
		return dig.hexdigest()

	async def __refreshPathHash(self,flags = ()):
		self.subpaths = ()
		t = self.getPathType(flags)
		if t == TYPE_NONE:
			self.subpaths = ()
			self.store["time"] = None
			self.store["hash"] = None
			return
		md = self.store.get("time",...)
		fmd = self.path.getModifiedTime()
		if fmd == md:
			return
		if t == TYPE_FILE:
			fhs = await self.checker.ws[AsyncOps].callInBackground(self.__computeFileHash)
		if t == TYPE_DIR:
			dig = hashlib.md5()
			self.subpaths = tuple(p for p in self.path.getChildren(deterministic = True) if self.checker.isFileRelevant(p))
			tasks = [fftask(self.checker.lookupCheckerByPath(p).__formatTypeAndName()) for p in self.subpaths]
			for t in tasks:
				dig.update(b'\0')
				dig.update(await t)
			fhs = dig.hexdigest()
		self.store["hash"] = fhs
		self.store["time"] = fmd
	
	async def __refreshSetHash(self, flags = ()):
		self.subpaths = tuple(p for p in self.path.findAll(deterministic = True) if self.checker.isFileRelevant(p))
		dig = hashlib.md5()
		tasks = [fftask(self.checker.lookupCheckerByPath(p).__formatPathAndHash()) for p in self.subpaths]
		for t in tasks:
			dig.update(b'\0')
			dig.update(await t)
		self.store["hash"] = dig.hexdigest()

	async def __refreshHashAndVersion(self, flags = ()):
		oldSubpaths = self.subpaths
		if isinstance(self.path,PathSet):
			await self.__refreshSetHash(flags)
		if isinstance(self.path,Path):
			await self.__refreshPathHash(flags)

		vn = self.revisions.get(self.store["hash"], None)
		if vn is None:
			self.store.pop("id",None)
		else:
			self.store["id"] = vn
		
		if oldSubpaths is not self.subpaths:
			for p in (k for k in oldSubpaths if k not in self.subpaths):
				ch = self.checker.lookupCheckerByPath(p)
				ch.hintDeleted()

	async def __refresh(self, flags = ()):
		while True:
			rt = self.refreshTask
			if rt is None:
				rt = Task(self.__refreshHashAndVersion(flags = flags))
				self.refreshTask = rt
			if rt.done():
				return
			await rt

	async def getHash(self, flags = ()) -> str:
		await self.__refresh(flags = flags)
		return self.store["hash"]

	async def getVersion(self):
		await self.__refresh()
		assert "hash" in self.store, "Cannot get version for file that does not exist!"
		vn = self.store.get("id",None)
		if vn is None: # Assign a new vn if absent.
			vn = self.checker._newVersion(self)
			self.revisions[self.store["hash"]] = vn
			self.store["id"] = vn
		return vn
	
	async def testVersion(self, version : int):
		await self.__refresh()
		return self.store.get("id",None) == version

	def refreshing(self):
		return (self.refreshTask is not None) and (not self.refreshTask.done())

	def clear(self):
		assert not self.refreshing()
		self.refreshTask = None
		if isinstance(self.path,PathSet):
			for p in self.subpaths:
				self.checker.lookupCheckerByPath(p).clear()
	
	def hintDeleted(self):
		aggressiveTask(self.__refresh(flags = ("deleted",)))

class FileDeltaChecker(Module):
	def __init__(self, context: Workspace) -> None:
		super().__init__(context)
		self.ws.add(Persistence)
		self.ws.add(AsyncOps)
	
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
	
	async def test(self, version : int):
		checker = self.__idmap.get(version,None)
		return checker is not None and await checker.testVersion(version)

	def clear(self, path: PathLike):
		path = self.__sanitizeQuery(path)
		ch = self.lookupCheckerByPath(path)
		ch.clear()

def manifest() -> Type[FileDeltaChecker]:
	return FileDeltaChecker

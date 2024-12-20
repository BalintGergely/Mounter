
from typing import Set, Dict
from functools import partial
from mounter.operation.core import *
from mounter.path import Path
from mounter.persistence import Persistence, persistenceTypeId
from mounter.delta import FileDeltaChecker
from mounter.progress import Progress

class FileManagement(Module):
	def __init__(self, context) -> None:
		super().__init__(context)
		self.ws.add(Persistence)
	
	def run(self):
		self.__store : Dict[str,Dict[str,Dict]] = self.ws[Persistence].lookup(self)
		self.__owner : Dict[str,str] = dict()
		try:
			self._downstream()
		finally:
			for (k,s) in self.__store.items():
				for v in [v for v in s.keys() if self.__owner.get(v,k) != k]:
					s.pop(v)
			self.__store = None
	
	def lock(self, path : Path, managingObject) -> Dict:
		"""
		Locks the specified path. This can and should be done exactly once per output path.
		Raises an exception if the path is already locked.
		Returns a persisted dictionary unique to the path and the type of the
		managing object. Useful for storing information on how the file was made.
		"""
		key = str(path)
		assert key not in self.__owner
		id = persistenceTypeId(managingObject)
		self.__owner[key] = id
		if id not in self.__store:
			self.__store[id] = dict()
		store = self.__store[id]
		if key not in store:
			store[key] = dict()
		return store[key]

	def __doCopy(sourcePath : Path, targetPath : Path):
		sourcePath.opCopyTo(targetPath)
	
	@op
	async def copyFile(self, sourcePath : Path, targetPath : Path):
		with self.ws[Progress].register() as pu:
			pu.setName(f"Copy {sourcePath} to {targetPath}")
			sourceHash = await self.ws[FileDeltaChecker].query(sourcePath)
			data = self.lock(targetPath,self)
			if sourceHash != data.get("sourceHash",None) \
			or not targetPath.isPresent():
				pu.setRunning()
				await self.ws[AsyncOps].callInBackground(partial(FileManagement.__doCopy,sourcePath,targetPath))
				data["sourceHash"] = sourceHash
			else:
				pu.setUpToDate()
		return targetPath
	
	def copyFileTo(self, sourcePath : Path, targetPath : Path):
		return self.copyFile(sourcePath, targetPath.subpath(sourcePath.getName()))

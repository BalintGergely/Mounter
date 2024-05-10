
from mounter.path import Path
from typing import Dict, Iterable, Hashable, List
from itertools import chain
import json
import hashlib
from mounter.operation import Operation
import mounter.workspace as workspace

class HashCache:
	'''
	A JSON serialized HashCache to use for checking state changes between builds.
	'''
	__table: Dict[str,Dict[str,str]] # The table, as loaded from the cache file. This never changes.
	__tableFile: Path # Location of the table file.
	__witness: Dict[str,Dict[str,str]] # Use this to store new information learned since load.

	def __init__(self,table: Path):
		self.__tableFile = Path(table)
		self.__table = None
		self.__witness = dict()
	
	def load(self):
		'''
		Load the hashcache from the file.
		'''
		assert self.__table is None, "Hash cache already loaded!"
		assert self.__witness is not None, "Hash cache already written!"
		assert not self.__tableFile.isDirectory(), "Hash cache must not be a directory!"
		if self.__tableFile.isFile():
			with self.__tableFile.open("r",encoding="utf-8") as input:
				self.__table = json.load(input)
		else:
			self.__table = {}
		
	def save(self):
		'''
		Save the hashcache.
		'''
		assert self.__table is not None, "Hash cache was never loaded!"
		savedTable = {}
		for k,v in chain(self.__table.items(),self.__witness.items()):
			if v.get("final",False):
				if k in savedTable:
					savedTable[k].update(v)
				else:
					savedTable[k] = dict(v)
				savedTable[k].pop("final")
		self.__tableFile.opCreateFile()
		with self.__tableFile.open("w",encoding="utf-8") as output:
			json.dump(
				obj=savedTable,
				fp=output,
				indent="\t")
	
	def toKeyStr(self,key : Hashable):
		'''
		Turn the key into a unique string representation.
		Key must be hashable, immutable.
		'''
		if isinstance(key,Path):
			checker = hashlib.sha1()
			checker.update(repr(key).encode())
			return str(checker.hexdigest())
		else:
			return None
	
	def processOpHash(self,opHash):
		'''
		Turn the opHash into a unique string representation.
		'''
		if isinstance(opHash,str):
			checker = hashlib.sha1()
			checker.update(opHash.encode())
			return str(checker.hexdigest())
		else:
			return None
	
	def _computeStateHash(self,key,renew,final):
		'''
		Compute the witness hash for the specific key.
		'''
		if isinstance(key,Path):
			
			if key.isDirectory():
				# Hash for directory is full hash including subpath names and children hashes.
				# The hash of a directory with no files is "emptydir".
				wils : List[Path] = list(key.getChildren())
				wils.sort()
				anyFileFound = False
				checker = hashlib.sha1()
				checker.update(b"\0")
				for wil in wils:
					if wil.getName() == "__pycache__":
						continue # Ignore pycache.
					childHash = self._updateState(wil, renew = renew, final = final)
					if childHash == "emptydir":
						continue
					checker.update(repr(wil).encode())
					checker.update(b"\0")
					checker.update(childHash.encode())
					checker.update(b"\0")
					anyFileFound = True
				if not anyFileFound:
					return "emptydir"
				return str(checker.hexdigest())

			if key.isFile():
				# Hash for file is the hash of the content... even for empty files!
				checker = hashlib.sha1()
				checker.update(b"\1")
				with key.open("r") as input:
					while True:
						data = input.read(65536)
						if not data:
							break
						checker.update(data)
				return str(checker.hexdigest())
			
			return "missing"
		return None

	def _updateState(self,key,renew : bool,final : bool,opHash : str = ...):
		assert self.__witness is not None, "Hash cache already written!"
		sk = self.toKeyStr(key)
		if sk is None:
			return None

		if sk not in self.__witness:
			self.__witness[sk] = dict()
		
		doRenew = renew or (not self.__witness[sk].get("final",False) and final)

		if doRenew or "stateHash" not in self.__witness[sk]:
			self.__witness[sk]["stateHash"] = self._computeStateHash(key, renew = renew, final = final)
			self.__witness[sk]["final"] = final
		if opHash is not ...:
			self.__witness[sk]["opHash"] = self.processOpHash(opHash)
		return self.__witness[sk]["stateHash"]
		
	def finalizeOutputState(self,key,opHash: str = ...):
		self._updateState(key,renew = True, final = True, opHash = opHash)
	
	def checkInputState(self,key):
		sk = self.toKeyStr(key)
		if sk is None:
			return False
			
		witnessHash = self._updateState(key, renew = False, final = True)

		return (
			sk in self.__table and
	  		"stateHash" in self.__table[sk] and
			witnessHash == self.__table[sk]["stateHash"]
		)

	def checkOutputState(self,key,opHash):
		sk = self.toKeyStr(key)
		if sk is None:
			return False
		
		if opHash is not ...:
			opHash = self.processOpHash(opHash)
			if sk not in self.__table:
				return False
			if "opHash" not in self.__table[sk]:
				return False
			if self.__table[sk]["opHash"] != opHash:
				return False
		
		witnessHash = self._updateState(key, renew = False, final = False)
		
		return (
			sk in self.__table and
	  		"stateHash" in self.__table[sk] and
			witnessHash == self.__table[sk]["stateHash"]
		)
	
	def manifest(self):
		return Module(self)

class Module(workspace.Module):
	_checker: HashCache

	def __init__(self, checker: HashCache = None):
		super().__init__(key = __file__)
		self._checker = checker
	
	def getChecker(self) -> HashCache:
		return self._checker
	
	def run(self,context):
		self._checker.load()
		try:
			context.run()
		finally:
			self._checker.save()

manifest = Module

class LazyOperation(Operation):
	__cache: HashCache
	__internal: Operation
	def __init__(self, internal: Operation, cache: HashCache):
		self.__cache = cache
		self.__internal = internal
	
	def getResultStates(self):
		return self.__internal.getResultStates()
	
	def getRequiredStates(self):
		return self.__internal.getRequiredStates()
	
	def getProgressLength(self):
		return self.__internal.getProgressLength()

	def __needsToRun(self):
		if not all([self.__cache.checkInputState(f) for f in self.__internal.getRequiredStates()]):
			return True
		myHash = self.opHash()
		if not all(self.__cache.checkOutputState(f,myHash) for f in self.__internal.getResultStates()):
			return True
		return False

	def __postRun(self):
		myHash = self.opHash()
		for f in self.__internal.getResultStates():
			self.__cache.finalizeOutputState(f,myHash)
	
	def __lazyProgress(self,progress):
		for _ in range(self.__internal.getProgressLength()):
			progress()

	async def runAsync(self, progress):
		if self.__needsToRun():
			result = await self.__internal.runAsync(progress)
			self.__postRun()
			return result
		else:
			self.__lazyProgress(progress)
	
	def run(self, progress):
		if self.__needsToRun():
			result = self.__internal.run(progress)
			self.__postRun()
			return result
		else:
			self.__lazyProgress(progress)
	
	def opHash(self):
		return self.__internal.opHash()
	
	def __str__(self):
		return "Lazy: "+str(self.__internal).replace("\n","\nLazy: ")


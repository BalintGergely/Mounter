
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
			with open(str(self.__tableFile), "r") as input:
				self.__table = json.load(input)
		else:
			self.__table = {}
		
	def save(self):
		'''
		Save the hashcache.
		'''
		assert self.__table is not None, "Hash cache was never loaded!"
		savedTable= {}
		for k,v in chain(self.__table.items(),self.__witness.items()):
			if k in savedTable:
				savedTable[k].update(v)
			else:
				savedTable[k] = dict(v)
		self.__tableFile.opCreateFile()
		with open(str(self.__tableFile), "w") as output:
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
			checker.update(str(key).encode())
			return str(checker.hexdigest())
		else:
			return None
	
	def processOpHash(self,opHash):
		if isinstance(opHash,str):
			checker = hashlib.sha1()
			checker.update(opHash.encode())
			return str(checker.hexdigest())
		else:
			return None
	
	def computeStateHash(self,key):
		'''
		Compute the witness hash for the specific key.
		This method performs the bulk of the computation, and does not store any result.
		'''
		if isinstance(key,Path):
			total = 0
			# Hash for directory is full hash including subpath names and children hashes.
			if key.isDirectory():
				wils : List[Path] = list(key.getChildren())
				wils.sort()
				checker = hashlib.sha1()
				for wil in wils:
					if wil.getName() == "__pycache__":
						continue # Exclude pycache.
					checker.update(str(wil).encode())
					checker.update(b"\0")
					checker.update(self.computeStateHash(wil).encode())
					checker.update(b"\0")
				return str(checker.hexdigest())
			# Hash for file is the hash of the content.
			if key.isFile():
				checker = hashlib.sha1()
				with open(str(key),"rb") as input:
					while True:
						data = input.read(65536)
						if not data:
							break
						total += len(data)
						checker.update(data)
				return str(checker.hexdigest())
		return None
	
	def getStateHash(self,key):
		'''
		If the state hash for the specified key has not been computed since the last load,
		computes the state hash and stores it for later invocations.
		The state hash is returned.
		'''
		assert self.__witness is not None, "Hash cache already written!"
		sk = self.toKeyStr(key)
		if sk is None:
			return None
		if sk not in self.__witness:
			self.__witness[sk] = dict()
		if "stateHash" not in self.__witness[sk]:
			self.__witness[sk]["stateHash"] = self.computeStateHash(key)
		return self.__witness[sk]["stateHash"]
	
	def checkState(self,key,opHash: str = ...) -> bool:
		'''
		If opHash is given, this first checks if opHash matches the stored opHash.
			If this check fails, False is returned immediately.
		
		Then, checks if the state hash for the given key has changed since the last save.
		The state hash is computed using getStateHash().

		If the test succeeds, True is returned. False otherwise.
		'''
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
		
		witnessHash = self.getStateHash(key)
		
		return (
			sk in self.__table and
	  		"stateHash" in self.__table[sk] and
			witnessHash == self.__table[sk]["stateHash"]
		)

	def setState(self,key,opHash: str = ...):
		'''
		Recomputes the state hash, discarding previous results related to the state.
		If opHash is given, it is assigned to the state.
		'''
		sk = self.toKeyStr(key)
		if sk is None:
			return
		if sk not in self.__witness:
			self.__witness[sk] = dict()
		if opHash is not ...:
			self.__witness[sk]["opHash"] = self.processOpHash(opHash)
		self.__witness[sk]["stateHash"] = self.computeStateHash(key)
	
	def manifest(self):
		return Module(self)

class Module(workspace.Module):
	_checker: HashCache

	def __init__(self, checker: HashCache = None):
		super().__init__(__file__)
		self._checker = checker
	
	def getChecker(self) -> HashCache:
		return self._checker
	
	def run(self,context):
		self._checker.load()
		context.run()
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

	def __needsToRun(self):
		if not all([self.__cache.checkState(f) for f in self.__internal.getRequiredStates()]):
			return True
		myHash = self.opHash()
		if not all(self.__cache.checkState(f,myHash) for f in self.__internal.getResultStates()):
			return True
		return False

	def __postRun(self):
		myHash = self.opHash()
		for f in self.__internal.getResultStates():
			self.__cache.setState(f,myHash)

	async def runAsync(self):
		if self.__needsToRun():
			result = await self.__internal.runAsync()
			self.__postRun()
			return result
	
	def run(self):
		if self.__needsToRun():
			result = self.__internal.run()
			self.__postRun()
			return result
	
	def opHash(self):
		return self.__internal.opHash()
	
	def __str__(self):
		return "Lazy: "+str(self.__internal).replace("\n","\nLazy: ")


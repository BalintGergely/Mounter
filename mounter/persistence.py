
import json
import time
from typing import List, Dict
from mounter.path import Path
from mounter.workspace import Module
from mounter.operation.loop import AsyncioLoop

def persistenceTypeId(obj):
	l : List[type] = list(type(obj).mro())[:-1]
	l.reverse()
	return "/".join(f"{k.__module__}.{k.__name__}" for k in l)

class Persistence(Module):
	"""
	This module provides a persistent data store which is periodically reset.
	"""
	def __init__(self, context) -> None:
		super().__init__(context)
		assert AsyncioLoop not in self.ws, (
			"""
When depending on both persistence and asyncio, please specify persistence as a dependency first.
"""
		)
		self._file : Path = None
		self._root : dict = None
		self.__self_id = persistenceTypeId(self)
	
	def setPersistenceFile(self, file : Path):
		assert self._file is None, "Persistence file may not be set more than once!"
		self._file = file
	
	def _loadPersistenceFile(self):
		assert self._root is None
		currentTime = time.time()

		reset = True
		
		if self._file.isFile():
			with self._file.open("r",encoding="utf-8") as input:
				data : dict = json.load(input)

			oldestAllowedTime = currentTime - 60 * 60 * 24 * 30

			if oldestAllowedTime < data[self.__self_id]["created"]:
				reset = False
		
		if reset:
			self._root = dict()
			self._root[self.__self_id] = {
				"created" : currentTime
			}
		else:
			self._root = data
	
	def _savePersistenceFile(self):
		assert self._root is not None
		s = json.dumps(self._root,ensure_ascii = False,sort_keys = True,separators=(',', ':'))
		with self._file.open("w",encoding="utf-8") as output:
			output.write(s)
		self._root = None
	
	def lookup(self, obj : Module) -> Dict:
		assert self._root is not None
		id = persistenceTypeId(obj)
		if id not in self._root:
			self._root[id] = dict()
		return self._root[id]
	
	def run(self):
		self._loadPersistenceFile()
		try:
			self._downstream()
		finally:
			self._savePersistenceFile()
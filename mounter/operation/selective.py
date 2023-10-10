
from typing import Iterable
import itertools
import mounter.hashcache as hashcache
from mounter.workspace import Workspace
from mounter.path import Path
import mounter.operation as operation
import mounter.operation.utility as utility

class Selective(operation.Module):

	def __init__(self, useAsync = False, goalStatePred = None, runSet = None, useHashCache = False):
		super().__init__(useAsync)
		self.goalStatePred = goalStatePred
		self.runSet = runSet
		self.useHashCache = useHashCache
	
	def activate(self, context : Workspace):
		if self.useHashCache:
			context.add(hashcache)
		return super().activate(context)
	
	def _runOperation(self, context, operation):

		for pt in (p for p in operation.getResultStates() if isinstance(p,Path)):
			pt : Path
			pt.getParent().opCreateDirectories()

		super()._runOperation(context, operation)

	def filterOperations(self, operations, context : Workspace):
		if self.goalStatePred is not None:
			filtered = list(utility.filterRequiredOperations(operations, self.goalStatePred))

			if len(filtered) == 0:
				print("No valid goals found! Printing all goal states:")

				for s in {k for k in utility.collectGoalRequirements(operations)}:
					print(s)
			
			operations = filtered


		(gates,nongates) = utility.separatePureGates(operations)
		gates = list(gates)
		nongates = list(nongates)

		hc = None

		if self.useHashCache:
			hc = context[hashcache].getChecker()
			nongates = (hashcache.LazyOperation(k,hc) for k in nongates)

		if self.runSet is not None:
			nnn = []
			for (i,k) in enumerate(nongates):
				k = operation.Gate(k,name=str(i))
				if str(i) not in self.runSet:
					k = operation.Silence(k)
				nnn.append(k)
			nongates = nnn
		
		operations = list(itertools.chain(gates,nongates))
		
		return operations			

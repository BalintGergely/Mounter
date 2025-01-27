
import shutil
import itertools
from typing import List
from mounter.operation.completion import isInterrupt
from mounter.workspace import *
from mounter.workspace import Workspace

TICK_PENDING = 0
TICK_RUNNING = 1
TICK_SKIPPED = 2
TICK_UP_TO_DATE = 3
TICK_DONE = 4
TICK_FAILED = 5
TICK_STOPPED = 6
NAME_SET = 6

def _divy(tickCount,maxTicks):
	maxTicks = min(tickCount,maxTicks)
	if maxTicks == 0:
		yield 0
		return
	for i in range(maxTicks + 1):
		yield (i * tickCount) // maxTicks

class ProgressUnit():
	def __init__(self, changeListener) -> None:
		self.__state = TICK_PENDING
		self.__r = changeListener
		self.__name = None
		self._pid = None
	
	def setName(self, name : str):
		self.__name = name
		self.__r(self,NAME_SET)
	
	def getName(self):
		return self.__name
	
	def setRunning(self):
		self.__state = TICK_RUNNING
		self.__r(self,TICK_RUNNING)
	
	def setUpToDate(self):
		self.__state = TICK_UP_TO_DATE
		self.__r(self,TICK_UP_TO_DATE)
	
	def getSortKey(self):
		if self.__state == TICK_PENDING:
			return 10
		return 0

	def getState(self):
		return self.__state
	
	def __enter__(self):
		return self

	def __exit__(self, exct, excc, excs):
		if excc is None:
			assert self.__state is not TICK_PENDING, \
				"ProgressUnit Must be marked as either running or up-to-date before exiting normally."
			if self.__state is TICK_RUNNING:
				self.__state = TICK_DONE
				self.__r(self, TICK_DONE)
		elif self.__state is TICK_PENDING:
			self.__state = TICK_SKIPPED
			self.__r(self, TICK_SKIPPED)
		elif self.__state is TICK_RUNNING:
			if isInterrupt(excc):
				self.__state = TICK_STOPPED
				self.__r(self, TICK_STOPPED)
			else:
				self.__state = TICK_FAILED
				self.__r(self, TICK_FAILED)

class Progress(Module):
	def __init__(self, context) -> None:
		super().__init__(context)
		self.__sequence : List[ProgressUnit] = []
		self.verbose = False
		self.__counter = 0
	
	def run(self):
		self.__printNonVerbose()
		try:
			self._downstream()
		finally:
			if not self.verbose:
				self.__printNonVerbose()
				print()
			self.__printFinalStatistics()

	def register(self):
		unit = ProgressUnit(self.__onUnitChange)
		self.__sequence.append(unit)
		self.__printNonVerbose()
		return unit
	
	def __count(self, type):
		return sum(x.getState() == type for x in self.__sequence)

	def __generateProgressString(self):
		self.__sequence.sort(key = ProgressUnit.getSortKey)
		max = len(self.__sequence)
		maxs = str(max)
		numLen = len(maxs)
		(termWidth,_) = shutil.get_terminal_size()

		dmax = min(termWidth,80) - 5 - numLen*2

		doneCount = self.__count(TICK_DONE)
		skipCount = self.__count(TICK_SKIPPED)
		failCount = self.__count(TICK_FAILED)
		utdCount = self.__count(TICK_UP_TO_DATE)

		istr = ""

		for (a,b) in itertools.pairwise(_divy(max, dmax)):
			subList = [p.getState() for p in self.__sequence[a:b]]
			if TICK_RUNNING in subList:
				istr += ">"
			elif TICK_STOPPED in subList:
				istr += "/"
			elif TICK_PENDING in subList:
				istr += " "
			elif TICK_FAILED in subList:
				istr += "!"
			elif TICK_SKIPPED in subList:
				istr += "."
			elif TICK_UP_TO_DATE in subList:
				istr += "="
			else:
				istr += "-"
		
		value = doneCount + skipCount + failCount + utdCount

		return (f"[{istr}]",f"{value:{numLen}}/{maxs}")

	def __printNonVerbose(self):
		(bar,per) = self.__generateProgressString()
		print(f" {bar} {per}",end='\r')

	def __onUnitChange(self, unit : ProgressUnit, changeType):
		if unit._pid is None:
			self.__counter += 1
			unit._pid = self.__counter
		if changeType == NAME_SET:
			return
		if changeType == TICK_STOPPED or (not self.verbose and changeType != TICK_FAILED):
			self.__printNonVerbose()
			return
		(bar,per) = self.__generateProgressString()
		unitName = unit.getName()
		if changeType == TICK_RUNNING:
			detail = f"[{unit._pid}] Started: {unitName}"
		elif changeType == TICK_DONE:
			detail = f"[{unit._pid}] Done."
		elif changeType == TICK_FAILED:
			detail = f"[{unit._pid}] Failed: {unitName}"
		elif changeType == TICK_SKIPPED:
			detail = f"[{unit._pid}] Skipped."
		elif changeType == TICK_UP_TO_DATE:
			detail = f"[{unit._pid}] Up to date."
		else:
			return
		print(f" {per} {detail}")
		if not self.verbose:
			self.__printNonVerbose()
	
	def __printFinalStatistics(self):
		nameList = [
			(TICK_PENDING,"pending"),
			(TICK_RUNNING,"running"),
			(TICK_DONE,"succeeded"),
			(TICK_UP_TO_DATE,"up to date"),
			(TICK_FAILED,"failed"),
			(TICK_SKIPPED,"skipped"),
			(TICK_STOPPED,"stopped")
		]
		reports = []
		for (t,n) in nameList:
			count = self.__count(t)
			if count != 0:
				if count == len(self.__sequence):
					count = "All"
				reports.append(f"{count} {n}")
		print(f", ".join(reports) + ".")


from typing import Set, Dict
from mounter.workspace import *
from mounter.workspace import Workspace

class GoalTracker(Module):
	def __init__(self, context) -> None:
		super().__init__(context)
		self.definedSet : Set[str] = set()
		self.goalSet : Dict[str,bool] = dict()
	
	def activateGoal(self,goal):
		self.goalSet[goal] = False
	
	def define(self,goal):
		assert goal not in self.definedSet, f"Duplicate definition for goal {goal}"
		self.definedSet.add(goal)
	
	def query(self,goal):
		assert goal in self.definedSet, f"Undefined goal {goal}"
		if goal in self.goalSet:
			self.goalSet[goal] = True
			return True
		return False
	
	def defineThenQuery(self,goal):
		self.define(goal)
		return self.query(goal)
	
	def run(self):
		self._downstream()
		for (k,v) in self.goalSet.items():
			if not v:
				print("WARNING: Unused goal: "+k)
		if not any(self.goalSet.values()):
			print("Printing all goals...")
			for k in self.definedSet:
				print(k)

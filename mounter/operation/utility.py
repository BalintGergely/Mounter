
from itertools import tee, chain
from typing import Iterable, Tuple
from mounter.operation.core import Operation, Gate

def separateInputs(operations: Iterable[Operation]) -> Iterable[Operation]:
	for op in operations:
		if isInput(op) and not isGoal(op):
			for f in op.getResultStates():
				yield Gate(produces=[f])
		else:
			yield op

def isGate(operation: Operation):
	'''Determine whether the specified operation is a gate.'''
	return isinstance(operation,Gate)

def isPureGate(operation: Operation):
	'''Determine whether the specified operation is a gate that has no internal operation.'''
	return isinstance(operation,Gate) and not operation.hasInternal()

def isGoal(operation: Operation):
	'''Determine whether the specified operation is a gate that is marked as a goal.'''
	return isinstance(operation,Gate) and operation.isGoal()

def isInput(operation: Operation):
	'''Determine whether the specified operation is a pure gate without any requirement states.'''
	return isPureGate(operation) and not operation.hasRequired()

def isOutput(operation: Operation):
	'''Determine whether the specified operation is a pure gate without any result states.'''
	return isPureGate(operation) and not operation.hasResult()

def collectGoalRequirements(operations: Iterable[Operation]):
	for op in operations:
		if isGoal(op):
			yield from op.getRequiredStates()

def separateGoals(operations: Iterable[Operation]) -> Tuple[Iterable[Gate],Iterable[Operation]]:
	td = tee(operations,2)
	return ((op for op in td[0] if isGoal(op)),(op for op in td[1] if not isGoal(op)))

def separatePureGates(operations: Iterable[Operation]) -> Tuple[Iterable[Gate],Iterable[Operation]]:
	td = tee(operations,2)
	return ((op for op in td[0] if isPureGate(op)),(op for op in td[1] if not isPureGate(op)))

def requiredOperations(operations: Iterable[Operation]) -> Iterable[Operation]:
	'''Filters out operations that do not contribute to goals.'''
	oneMore = True
	requiredStates = set()
	opList = list(operations)
	while oneMore:
		oneMore = False
		for i in range(len(opList)):
			op = opList[i]
			if op is not None:
				if isGoal(op) or any((f in requiredStates) for f in op.getResultStates()):
					yield op
					opList[i] = None
					oneMore = True
					requiredStates.update(op.getRequiredStates())

def filterOrUnwrapGoals(ops: Iterable[Gate], goalState) -> Iterable[Operation]:
	for op in ops:
		if isGoal(op) and not any(goalState(s) for s in op.getRequiredStates()):
			yield op.nonGoal()
		else:
			yield op

def filterRequiredOperations(operations : Iterable[Operation], goalState) -> Iterable[Operation]:
	return requiredOperations(filterOrUnwrapGoals(operations, goalState))

uniqueStateCounter = int(0)

def uniqueState(name = ...):
	s = uniqueStateCounter
	uniqueStateCounter = s + 1
	if name is ...:
		return f"unique state {s}"
	else:
		return f"unique state {s} {name}"

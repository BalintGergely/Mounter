
from mounter.operation.completion import *
from mounter.workspace import Module

class Guardian(Module):
	def __init__(self, context):
		super().__init__(context)
		self.__taskList : List[Future] = []
	
	def run(self):
		self._downstream()	
		self.__checkAllTasks()
	
	def __checkAllTasks(self):
		for t in self.__taskList:
			t.result()
	
	def completeLater(self, t : Future):
		self.__taskList.append(Task(t))

def once(fun : Callable[P,A]) -> Callable[P,A]:
	"""
	The decorated method is run only once per unique set of arguments.
	This does NOT support default arguments.
	"""
	attrName = f"op{id(fun)}"
	@functools.wraps(fun)
	def wrapper(self, *args, **kwargs):
		key = (args,frozenset(kwargs.items()))
		cache = getattr(self, attrName, None)
		if cache is None:
			cache = dict()
			setattr(self, attrName, cache)
		if key not in cache:
			cache[key] = fun(self, *args)
		return cache[key]
	return wrapper

def task(coro : Callable[P,Awaitable[A]]) -> Callable[P,CompletionFuture[A]]:
	"""
	The decorated coroutine is wrapped in a completion Task.
	The task is explicitly not fail-fast.
	"""
	@functools.wraps(coro)
	def wrapper(*args,**kwargs):
		return Task(coro(*args,**kwargs))
	return wrapper

def operation(fun : Callable[P,Awaitable[A]]) -> Callable[P,CompletionFuture[A]]:
	"""
	A composition of the once and task decorators, with the addition of fail-fast behaviour.
	The decorated async method is ran asynchronously once per unique set of arguments.
	
	If it fails before the decorated method returns to the caller, the exception is raised immediately.
	This behaviour can be modified by setting the "failFast" keyword argument to false.
	"""
	fun = once(task(fun))
	@functools.wraps(fun)
	def wrapper(*args,failFast : bool = True,**kwargs):
		k = fun(*args,**kwargs)
		if failFast and k.failed():
			raise k.exception()
		return k
	return wrapper

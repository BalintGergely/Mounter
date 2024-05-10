
from mounter.path import Path
import re

class FilteredPath(Path):
	def __init__(self, path, pattern : re.Pattern):
		super().__init__(path)
		if type(pattern) == re.Pattern:
			self.__f = pattern
		else:
			self.__f = re.compile(pattern)
	
	def __hash__(self):
		return super().__hash__() ^ self.__f.__hash__()
	
	def __eq__(self,other):
		return super().__eq__(other) and self.__f == other.__f

	def getChildren(self):
		return (FilteredPath(p,self.__f) for p in super().getChildren() if p.isDirectory() or self.__f.fullmatch(str(p)))
	
	def __repr__(self):
		return f"FilteredPath(\'{str(self)},{self.__f.pattern}\')"
	
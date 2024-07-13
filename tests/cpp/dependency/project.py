
import tests.cpp.lib.project as lib
from mounter.languages.cpp import CppProject

class manifest(CppProject):
	def __init__(self,context):
		super().__init__(context,__file__,lib)
		self.mains.add("main.cpp")
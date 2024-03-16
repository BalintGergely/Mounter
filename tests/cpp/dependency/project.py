
import tests.cpp.lib.project as lib
from mounter.languages.cpp import CppProject

class manifest(CppProject):
	def __init__(self):
		super().__init__(__file__,lib)
		self._main = "main.cpp"
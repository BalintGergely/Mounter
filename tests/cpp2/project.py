
import mounter.operation as op
from mounter.workspace import Workspace
from mounter.languages.cpp import CppGroup, ClangModule, CppProject
import tests.cpp.project as cpp1

class manifest(CppProject):
	def __init__(self):
		super().__init__(__file__, cpp1)

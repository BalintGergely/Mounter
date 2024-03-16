
from mounter.languages.cpp import CppProject

class manifest(CppProject):
	def __init__(self):
		super().__init__(__file__)
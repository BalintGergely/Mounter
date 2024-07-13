
from mounter.languages.cpp import CppProject

class manifest(CppProject):
	def __init__(self,context):
		super().__init__(context,__file__)
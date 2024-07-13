
from mounter.operation import AsyncOps
from mounter.operation.files import FileManagement
from mounter.languages.cpp import CppGroup, CppProject

class manifest(CppProject):
	def __init__(self, context):
		super().__init__(context, __file__)
		self.mains.add("main.cpp")
	
	async def onCompile(self, mainGroup: CppGroup):
		self.ws[AsyncOps].completeLater(
			self.ws[FileManagement].copyFileTo(
				self._dir.subpath("resource.txt"),
				await mainGroup.getBinDirectory()
			)
		)

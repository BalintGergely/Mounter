
import mounter.operation as op
from mounter.languages.java import JavaProject, JavaGroup

class manifest(JavaProject):
	def __init__(self):
		super().__init__(__file__)

	def fillGroup(self, group: JavaGroup):
		group.addResourceFiles(self._path,"txt")
		super().fillGroup(group)
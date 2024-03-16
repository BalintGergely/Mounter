
import mounter.operation as op
from mounter.languages.java import JavaProject

class manifest(JavaProject):
	def __init__(self):
		super().__init__(__file__)

	def fillGroup(self, group: CppGroup, context : Workspace):
		opmod : op.Module = context[op]
		clang : ClangModule = context[ClangModule]
		for p in self.collectSources():
			if p.hasExtension("txt"):
				t = p.relativeTo(self._path).moveTo(clang.bin)
				opmod.add(op.Copy(p,t))
				group.addGoal(t)

		return super().fillGroup(group, context)
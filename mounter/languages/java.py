
from mounter.path import *
from mounter.operation.util import *


"""

The Java compiler has TWO modes.

"Basic mode"
- Standard.
- Cannot compile multiple modules at the same time

"Multi module mode"
- Compile multiple modules in one go
- Each module goes to separate directory in output
- Must specify the name of all modules

Ideally we want to run the compiler once... so what to do?

- A list of .java files to be compiled
- - Will we suppress generation of unmentioned class files?

- The destination directory. Where to put generated .class files.
- - Will be arranged in a PACKAGE hierarchy

- The destination header directory. Where to put generated .h files. (C headers)
- - Layout unclear
- - One file per class with native method

- The destination source directory. Where to put generated source files. (...by annotation processors.)

- A set of source paths for Java to reference
- - These are directories where Java can look for SOURCE files

- A set of class paths for Java to reference
- - These are directories where Java can look for CLASS files

- A set of annotation processors to run
- - Full class names. These must be already compiled.

- A set of module paths
- - Each is a path to a module or directory of modules

"""

class JavaGroup():
	def __init__(
			self,
			moduleSourcePaths : Dict[str,Path],
			outputPath : Path,
			headerOutputPath : Path,
			generatedOutputPath : Path
			):
		pass
	pass
	
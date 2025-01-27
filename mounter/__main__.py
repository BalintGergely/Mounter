
import argparse
import mounter.languages.cpp as cpp
import importlib
from mounter.workspace import Workspace
from mounter.path import Path
from mounter.persistence import Persistence
from mounter.goal import GoalTracker
from mounter.progress import Progress
from mounter.operation import AsyncOps
from mounter.operation.completion import isInterrupt
from mounter.exceptions import BuildException

root = Path("")
obj = Path("obj")
bin = Path("bin")

parser = argparse.ArgumentParser()
parser.add_argument('project', type=str, help='The project to build.')
parser.add_argument('goals', type=str, help='The goals to build', nargs='*')
parser.add_argument('--hustle', action="store_true", help="Wait for user confirmation at the beginning.")
parser.add_argument('--verbose', action="store_true", help="Print detailed information on what mounter is doing.")
parser.add_argument('--disassembly', action="store_true", help="Use textual intermediate representation, wherever applicable.")
parser.add_argument('--debug', action="store_true", help="Compile debug information, wherever applicable.")
parser.add_argument('--optimalize', action="store_true", help="Enable optimalizations.")
parser.add_argument('--sequential', action="store_true", help="Use deterministic sequential execution.")

w = Workspace()

args = parser.parse_args()

if args.hustle:
	result = input("Press enter to continue > ")
	if result != "":
		print("Halting because non-empty input string given")
		exit()

w.use(cpp.ClangModule)
w.add(importlib.import_module(args.project))

obj.opCreateDirectories()
bin.opCreateDirectories()

if Progress in w:
	progress = w[Progress]
	progress.verbose = args.verbose

if GoalTracker in w:
	goalTracker = w[GoalTracker]
	for g in args.goals:
		goalTracker.activateGoal(g)

if Persistence in w:
	persistence = w[Persistence]
	persistence.setPersistenceFile(obj.subpath("mounterPersist.json"))

if AsyncOps in w:
	ops = w[AsyncOps]
	if args.sequential:
		ops.disableAsync()

if cpp.ClangModule in w:
	cppManifest = w[cpp.ClangModule]

	cppManifest.rootDirectory = root
	cppManifest.binDirectory = bin
	cppManifest.objDirectory = obj.subpath("cpp")
	cppManifest.assemble = args.disassembly
	cppManifest.debug = args.debug
	cppManifest.optimalize = args.optimalize

try:
	w.run()
except BaseException as x:
	if isInterrupt(x):
		print("Interrupted")
	elif isinstance(x,BuildException):
		x.report()
	else:
		raise x
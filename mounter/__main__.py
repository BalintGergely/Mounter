
import argparse
import mounter.operation as operation
import mounter.languages.cpp as cpp
import mounter.languages.java as java
import mounter.hashcache as hashcache
import importlib
from mounter.workspace import Workspace
from mounter.path import Path

root = Path("")
obj = Path("obj")
bin = Path("bin")

parser = argparse.ArgumentParser()
parser.add_argument('project', type=str, help='The project to build.')
parser.add_argument('main', type=str, help='The main goal file name.')
parser.add_argument('--whatif', metavar='Id', type=str, nargs='*', help='Specify to run no or select operations.')
parser.add_argument('--noasync', action="store_true", help="Disable asynchronous build.")
parser.add_argument('--disassembly', action="store_true", help="Use textual intermediate representation, wherever applicable.")
parser.add_argument('--debug', action="store_true", help="Compile debug information, wherever applicable.")
parser.add_argument('--hustle', action="store_true", help="Wait for user confirmation at the beginning.")
parser.add_argument('--optimalize', action="store_true", help="Enable optimalizations.")

w = Workspace()

args = parser.parse_args()

if args.hustle:
	result = input("Press enter to continue > ")
	if result != "":
		print("Halting because non-empty input string given")
		exit()

def get_project(project : str):
	if project == ".":
		project_string = "project"
	else:
		project_string = project+".project"
	
	module = importlib.import_module(project_string)
	
	return module

project = get_project(args.project)

obj.opCreateDirectories()
bin.opCreateDirectories()

w.use(hashcache.HashCache(obj.subpath("hashCache.json")).manifest())

def isWantedGoal(state):
	if isinstance(state,Path):
		state = str(state)
		key = state.split("/")[-1]
		return args.main.endswith(key) and state.endswith(key)
	else:
		return args.main == state

opm: operation.Module = w.use(operation.Selective(
	useAsync = not args.noasync,
	goalStatePred = isWantedGoal,
	runSet = set(args.whatif) if args.whatif is not None else None,
	useHashCache = True,
	printProgress = True
))

cppManifest = w.use(cpp.ClangModule(root=root,obj=obj.subpath("cpp"),bin=bin))

cppManifest.assemble = args.disassembly
cppManifest.debug = args.debug
cppManifest.optimalize = args.optimalize

javaManifest = w.use(java.Module(root=root,obj=obj.subpath("java"),include=obj.subpath("javaInclude"),bin=bin))
javaManifest.debug = args.debug

w.add(project)

w.run()


from itertools import chain
from mounter.path import Path

paths = [Path("obj"),Path("bin")]
paths = [p for p in paths if p.isPresent()]

for p in paths:
	for k in p.getPostorder():
		k.opDelete()

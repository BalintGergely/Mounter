from typing import Iterable, Tuple
from mounter.path import Path, RelativePath
from mounter.operation.core import Operation

class ZipOperation(Operation):
	def __init__(self, target : Path, files : Iterable[RelativePath]) -> None:
		
		super().__init__()
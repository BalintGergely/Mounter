
from mounter.path import Path
import mounter.workspace as workspace
import subprocess
import re
from typing import Any, Dict, Set

from mounter.workspace import Workspace

words = re.compile(r"[^\s]+")
lines = re.compile(r"[^\r\n]+")
refname = re.compile(r"(?!\.)(?:(?!\.\.|@{|[ :\\?[^~*])[\x20-\xFE])+(?<!/)(?<!\.lock)")
objectid = re.compile(r"[0-9a-fA-F]{40}")
refspec = re.compile(r"(\+)?(\w+)\:(\w+)")

"""

... means the information is unknown
None means the information is known to be unset

"""

class Git(workspace.Module):
	def __init__(self, gitDir : Path = Path("./.git")) -> None:
		super().__init__((__file__,gitDir))
		self.__baseCommand = ("git",f"--git-dir={gitDir}")
		self.__remoteList = ...
		self.__referenceList = ...
	
	def __cmd(self, *query, stdin = None, work_tree : Path = ...):
		command = list(self.__baseCommand)
		if work_tree is not ...:
			command.append(f"--work-tree={str(work_tree)}")
		command.extend(query)
		proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		stdout, stderr = proc.communicate(input = stdin)
		if proc.returncode != 0:
			raise Exception(f"{subprocess.list2cmdline(command)}\r\n{stderr.decode()}")
		return stdout
	
	def __cmdcode(self, *query, stdin = None):
		command = self.__baseCommand + tuple(str(q) for q in query)
		proc = subprocess.Popen(command)
		proc.communicate(input = stdin)
		return proc.returncode

	def __cmdlines(self, *query, stdin = None):
		return lines.findall(self.__cmd(query, stdin))
	
	def remote(self):
		if self.__remoteList is ...:
			self.__remoteList = set(self.__cmdlines("remote"))
		yield from self.__remoteList
	
	def configRemoteSetURL(self, remote : str, url : str):
		self.__cmd("config",f"remote.{remote}.url",url)
		if self.__remoteList is not ...:
			self.__remoteList.add(remote)
	
	def getReference(self, reference : str, cacheonly : bool = False):
		if self.__referenceList is ...:
			value = ...
		else:
			value = self.__referenceList.get(reference, None)
		if value is ... and not cacheonly:
			self.__referenceList = {k:v for v,k in (words.findall(line) for line in self.__cmdlines("show-ref"))}
			value = self.__referenceList.get(reference, None)
		return value

	def setReference(self, reference : str, newValue : str):
		oldValue = self.getReference(reference, cacheonly = True)

		if oldValue == newValue:
			return
		
		if oldValue == None:
			oldValue = ""

		if newValue is None:
			if oldValue is ...:
				self.__cmd("update-ref","-d",reference)
			else:
				self.__cmd("update-ref","-d",reference,oldValue)
		else:
			if oldValue is ...:
				self.__cmd("update-ref",reference,newValue)
			else:
				self.__cmd("update-ref",reference,newValue,oldValue)

	def testObjectExists(self, objectId : str):
		return self.__cmdcode("cat-file","-e",objectId) == 0
	
	def fetch(self, remote : str, remoteReference : str, localReference : str, depth : int = ...):
		args = ["fetch",remote,f"{remoteReference}:{localReference}","--no-tags"]
		if depth is not ...:
			args.append(f"--depth={depth}")
		self.__cmd(*args)
		self.__referenceList[localReference] = ...
	
	def checkout(self, revision : str, path : Path):
		self.__cmd("checkout",revision,".",work_tree=path)

class Fetch:
	def __init__(self,
			remoteUrl : str = ...,
			remoteName : str = ...,
			saveRemote : bool = ...,
			remoteReference : str = ...,
			objectId : str = ...,
			localReference : str = ...) -> None:
		self.__remote_url = remoteUrl
		self.__remote_name = remoteName
		self.__save_remote = saveRemote
		self.__remote_reference = remoteReference
		if objectId is ...:
			if objectid.fullmatch(remoteReference):
				objectId = remoteReference
		self.__object_id = objectId
		self.__local_reference = localReference
	
	def __call__(self, git : Git) -> Any:
		currentObject = git.getReference(self.__local_reference)

		if currentObject is not None:
			if self.__object_id is not ...:
				assert currentObject == self.__object_id
			return

		if self.__object_id is not ...:
			if git.testObjectExists(self.__object_id):
				git.setReference(self.__local_reference,self.__object_id)
				return

		remoteIdentity = None

		if self.__remote_name in git.remote():
			remoteIdentity = self.__remote_name
		elif self.__save_remote:
			git.configRemoteSetURL(self.__remote_name,self.__remote_url)
			remoteIdentity = self.__remote_name
		else:
			remoteIdentity = self.__save_remote
		
		git.fetch(remoteIdentity, self.__remote_reference, self.__local_reference, depth = 1)

		if self.__object_id is not ...:
			currentObject = git.getReference(self.__local_reference)
			if currentObject != self.__object_id:
				if git.testObjectExists(self.__object_id):
					git.setReference(self.__local_reference,self.__object_id)
					return
				else:
					git.setReference(self.__local_reference,None)
					assert False

class Checkout:
	"""
	It is recommended to use git revision format for objects. For example: library^{tree}:src/main
	"""
	def __init__(self, revision : str, target : Path) -> None:
		self.__revision = revision
		self.__target = target
		pass

	def __call__(self, git : Git):
		git.checkout(self.__revision,self.__target)
		pass

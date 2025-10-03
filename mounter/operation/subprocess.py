
from typing import Tuple
import asyncio

from mounter.operation.completion import *
import mounter.workspace as ws
from mounter.operation.loop import AsyncioLoop
from mounter.operation.bridge import Bridge, BridgeShutdownException
from mounter.operation.protocol import *

from asyncio.subprocess import PIPE, DEVNULL

class SubprocessManager(ws.Module):
	def __init__(self, context):
		super().__init__(context)
		self.ws.add(Bridge)
	
	def __makeSubprocessProtocol(self):
		return SubprocessBridgeProtocol(self.ws[Bridge])
	
	async def startSubprocess(self,
	                          command : Tuple[str],
	                          stdin=PIPE,
							  stdout=PIPE,
							  stderr=PIPE
							  ) -> Subprocess:
		bridge = self.ws[Bridge]

		if bridge.isShutdown():
			return bridge.onShutdown()
	
		asyncio.create_subprocess_exec
		
		_, protocol = await bridge.bridgeAsync(
			self.ws[AsyncioLoop].loop.subprocess_exec(
				self.__makeSubprocessProtocol,
				command[0],
				*command[1:],
				stdin=stdin,
				stdout=stdout,
				stderr=stderr
			)
		)

		return Subprocess(protocol)


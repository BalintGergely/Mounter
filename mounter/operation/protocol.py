
import warnings
import asyncio.protocols as p
import asyncio.transports as t
from mounter.operation.completion import *
from mounter.operation.bridge import *

class BridgeBaseProtocol(p.BaseProtocol):
	def __init__(self, bridge : Bridge):
		super().__init__()
		self._bridge = bridge
		self.__connectionMadeFuture = CompletableFuture()
		self.onOpen = bridge.bridgeTask(self.__connectionMadeFuture)
		self.__connectionLostFuture = CompletableFuture()
		self.onClose = bridge.bridgeTask(self.__connectionLostFuture)
	
	def connection_made(self, transport : t.BaseTransport):
		self.__connectionMadeFuture.setResult(None)
		if transport is None:
			self.__connectionLostFuture.setResult(None)
		else:
			self._transport = transport
			self.onClose.thenCall(self.__cleanup)

	def connection_lost(self, exc):
		if exc is not None:
			self.__connectionLostFuture.setException(exc)
		else:
			self.__connectionLostFuture.setResult(None)

	def close(self):
		self.__cleanup()
	
	def __cleanup(self):
		if not self._transport.is_closing():
			self._transport.close()

class StreamReaderProtocol(p.Protocol,BridgeBaseProtocol):
	def __init__(self, bridge):
		self._transport : t.ReadTransport
		super().__init__(bridge)
		self.__data_queue : List[bytes] = []
		self.__eof_or_closed = False
		self.__consumer_queue = QueueDelayer()
		self.__reading_paused = False
		self.onClose.thenCall(self.__cleanup)
	
	async def read(self):
		if len(self.__data_queue) == 0 or self.__consumer_queue.waiting():

			if self.__eof_or_closed:
				return None
			
			if self.__reading_paused:
				self.__reading_paused = False
				self._transport.resume_reading()
			
			if self.__consumer_queue is None:
				return None
			
			await self.__consumer_queue

			if len(self.__data_queue) == 0:
				return None

		return self.__data_queue.pop(0)

	def data_received(self, data : bytes):
		self.__data_queue.append(bytes(data))

		while len(self.__data_queue) != 0 and self.__consumer_queue.waiting():
			self.__consumer_queue.run(1)

		if len(self.__data_queue) != 0 and not self.__reading_paused:
			self.__reading_paused = True
			self._transport.pause_reading()

	def eof_received(self):
		self.__cleanup()
		return True

	def __cleanup(self):
		self.__eof_or_closed = True
		self.__consumer_queue.run()

class StreamWriterProtocol(BridgeBaseProtocol):
	def __init__(self, bridge):
		self._transport : t.WriteTransport
		super().__init__(bridge)
		self.__writing_queue = QueueDelayer()
		self.__writing_paused = True
		self.__eof_written_or_closed = False
		self.onClose.thenCall(self.__cleanup)
	
	def connection_made(self, transport):
		super().connection_made(transport)
		self.resume_writing()
	
	async def write(self,data : bytes | bytearray | memoryview):
		if self.__writing_paused or self.__writing_queue.waiting():

			if self.__eof_written_or_closed:
				return False

			await self.__writing_queue

			if self.__eof_written_or_closed:
				return False
		
		self._transport.write(data)
		return True
	
	async def write_eof(self):
		if self.__writing_paused or self.__writing_queue.waiting():

			if self.__eof_written_or_closed:
				return False

			await self.__writing_queue

			if self.__eof_written_or_closed:
				return False

		try:
			self._transport.write_eof()
		finally:
			self.__cleanup()
		
		return True
	
	def pause_writing(self):
		self.__writing_paused = True

	def resume_writing(self):
		if self.__eof_written_or_closed:
			return
		
		self.__writing_paused = False

		while not self.__writing_paused and self.__writing_queue.waiting():
			self.__writing_queue.run(1)
	
	def __cleanup(self):
		self.__writing_paused = True
		self.__eof_written_or_closed = True
		self.__writing_queue.run()

class SubprocessBridgeProtocol(BridgeBaseProtocol,p.SubprocessProtocol):
	def __init__(self, bridge):
		self._transport : t.SubprocessTransport
		super().__init__(bridge)
		self.stdin : StreamWriterProtocol = StreamWriterProtocol(bridge)
		self.stdout : StreamReaderProtocol = StreamReaderProtocol(bridge)
		self.stderr : StreamReaderProtocol = StreamReaderProtocol(bridge)
		self.__pipe_protocols = (self.stdin,self.stdout,self.stderr)
		self.__process_exit = CompletableFuture()
		self.onProcessExit = bridge.bridgeTask(self.__process_exit)
	
	def connection_made(self, transport : t.SubprocessTransport):
		for (fd,p) in enumerate(self.__pipe_protocols):
			t = transport.get_pipe_transport(fd)
			p.connection_made(t)

		super().connection_made(transport)
	
	def pipe_data_received(self, fd, data):
		self.__pipe_protocols[fd].data_received(data)

	def pipe_connection_lost(self, fd, exc):
		self.__pipe_protocols[fd].connection_lost(exc)

	def process_exited(self):
		self.__process_exit.setResult(self._transport.get_returncode())
	
	def terminate(self):
		self._transport.terminate()

class StreamReader():
	def __init__(self, protocol : StreamReaderProtocol):
		self.__protocol = protocol
	
	def read(self):
		return self.__protocol.read()

class StreamWriter():
	def __init__(self, protocol : StreamWriterProtocol):
		self.__protocol = protocol
	
	def write(self, data : bytes | bytearray | memoryview):
		return self.__protocol.write(data)
	
	def close(self):
		return self.__protocol.write_eof()

class Subprocess():
	def __init__(self, protocol : SubprocessBridgeProtocol):
		self.__protocol = protocol

		if protocol.stdin is not None:
			self.__stdin = StreamWriter(protocol.stdin)
		
		if protocol.stdout is not None:
			self.__stdout = StreamReader(protocol.stdout)
		
		if protocol.stderr is not None:
			self.__stderr = StreamReader(protocol.stderr)
		
		self.__returnCode = protocol.onProcessExit
	
	@property
	def stdin(self) -> StreamWriter:
		return self.__stdin

	@property
	def stdout(self) -> StreamReader:
		return self.__stdout
	
	@property
	def stderr(self) -> StreamReader:
		return self.__stderr
	
	def waitOpen(self):
		return self.__protocol.onOpen

	def waitExit(self):
		"""
		Wait for the subprocess to exit.
		"""
		return self.__returnCode
	
	def terminate(self):
		"""
		Terminate the subprocess.
		"""
		self.__protocol.terminate()

async def readAllBytes(input : StreamReader) -> bytearray:
	"""
	Read all bytes from the stream reader.
	"""
	buffer = bytearray()

	while True:
		data = await input.read()
		if data is None:
			break

		buffer.extend(data)
	
	return buffer

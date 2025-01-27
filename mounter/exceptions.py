
class BuildException(Exception):
	def __init__(self, message = ""):
		super().__init__()
		self.__message = message
	
	def report(self):
		print(self.__message)
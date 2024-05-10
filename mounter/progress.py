import shutil
import time

def progressTick(value,max):
	if(max == 0):
		print(" [] 0/0")
	else:
		assert 0 <= value and value <= max
		maxs = str(max)

		(termWidth,_) = shutil.get_terminal_size()

		dmax = min(termWidth,80) - 5 - len(maxs)*2
		dmax = min(dmax,max)
		dval = (value * dmax) // max
		print(" [{}{}]".format("=" * (dval)," " * (dmax - dval)),end="")
		print(f" {value:{len(maxs)}}/{maxs}",end="\r")

def progressInit(max):
	progressTick(0,max)

def progressEnd():
	print("",end="\n")

if __name__ == "__main__":
	max = 100
	progressInit(max)
	for i in range(max):
		time.sleep(0.5)
		progressTick(i + 1,max)
	progressEnd()

class progress():
	def __init__(self,max) -> None:
		progressInit(max)
		self.__max = max
		self.__value = 0
	
	def __call__(self):
		self.__value += 1
		progressTick(self.__value,self.__max)

	def end(self):
		progressEnd()

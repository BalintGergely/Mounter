
import site
from mounter.path import Path

mounterPath = Path(__file__).getParent().subpath("mounter")
targetPath = Path(site.getusersitepackages()).subpath("mounter")

print("This will copy the contents of the 'mounter' directory to the following location:")
print()
print(str(targetPath))
print()
print("THIS OPERATION WILL OVERRIDE WHATEVER IS ALREADY THERE.")
print()
response = input("Type 'y' to continue > ")

if response.lower() != 'y':
	print("Operation canceled.")
	exit()

for f in list(targetPath.getPostorder(includeSelf=False)):
	f.opDelete()
	print(f"[deleted {f}]")

if not targetPath.isDirectory():
	targetPath.opCreateDirectory()

for m in mounterPath.getPreorder(includeSelf=False):
	m = m.relativeTo(mounterPath)
	t = m.moveTo(targetPath)
	if "__pycache__" not in m.relativeStr():
		if m.isDirectory():
			t.opCreateDirectory()
			print(f"[created {t}]")
		if m.isFile():
			m.opCopyTo(t)
			print(f"[wrote   {t}]")

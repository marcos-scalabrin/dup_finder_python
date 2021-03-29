import os,sys,inspect
print(os.getcwd())
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
print(os.path.abspath('.')+'\n\n')
print(currentdir)
print('\n\n')
parentdir = os.path.dirname(currentdir)
print(sys.path)
print('\n\n')
sys.path.insert(0,parentdir+'/src') 
print(sys.path)


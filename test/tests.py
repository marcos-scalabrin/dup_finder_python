import os,sys,inspect
# inspect garante que caminho será referente à esse arquivo e não de onde foi
# rodado o python. 
# premissa é que códigos (/src) e testes (/test) estã no mesmo nivel
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir+'/src') 

import 

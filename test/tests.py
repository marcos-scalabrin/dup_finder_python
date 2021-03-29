import os,sys,inspect
# inspect garante que caminho será referente à esse arquivo e não de onde foi
# rodado o python. 
# premissa é que códigos (/src) e testes (/test) estã no mesmo nivel
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir+'/src') 

import dup_finder as dfr
from utils import *


def test_base_path():
    curdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    ts_run = dt.now()
    dfr.main('.',ts_run)

def exec_command(sql):
    is_success = None
    try:
        conn = psycopg2.connect(static_connect_str)
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        if cursor.rowcount > 0: is_success = True
    except Exception as e:
        print('\n\n',type(e), e,'\n\n')
        is_success = False
    finally:
        if conn:
            cursor.close()
            conn.close()
    return False if is_success is None else is_success

def test_get_rows():
    sql = 'select unnest(array[0,1,2,3,4]) as valor'
    rows = dfr.get_rows(sql)
    cnt_rows = 0 
    values = [0,1,2,3,4]
    for row in rows:
        assert row[0] == values[cnt_rows]
        cnt_rows += 1
    assert cnt_rows == 5

def test_get_rows_01():
    sql = 'select * from dup_finder.file limit 100'
    rows = dfr.get_rows(sql)
    assert len(rows) == 100


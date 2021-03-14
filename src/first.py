from hashlib import md5
from utils import ic, Timer,ts_pp_to
import os, sys
from datetime import datetime as dt
from uuid import UUID
import psycopg2
from tqdm import tqdm

# database data
# TODO argumentos, config.file
PSQL_USER = 'postgres'
PSQL_PSWD = 'local_pswd_2020'
PSQL_HOST = 'localhost'
PSQL_PORT = '5432'
PSQL_DB   = 'postgres'

static_connect_str = f'postgresql://{PSQL_USER}:{PSQL_PSWD}@{PSQL_HOST}:{PSQL_PORT}/{PSQL_DB}'

def hex_2_uuid(hd):
    return UUID(hd)

def get_file_hd(file,chunksize=128*10000):
    with open(file, "rb") as f:
        file_hash = md5()
        while chunk := f.read(chunksize):
            file_hash.update(chunk)
    return file_hash.hexdigest()

def ts_to_dt(ts):
    return dt.fromtimestamp(ts)

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

def check_dir_processed(root):
    """ checa de diretorio ja foi processado
    """
    try:
        conn = psycopg2.connect(static_connect_str)
        cursor = conn.cursor()
        cursor.execute(
            'select %(root)s in (select distinct file_path from dup_finder.file)', 
            {'root':root} ) 
        conn.commit()
        rows = cursor.fetchall()
        for row in rows:
            post_id = row[0]
    except Exception as e:
        print(type(e), e)
    finally:
        if conn:
            cursor.close()
            conn.close()
    return post_id

def record_dir_data(data:dict):
    len(data)

def record_file_data(data:dict):
    try:
        conn = psycopg2.connect(static_connect_str)
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO dup_finder.file ' 
            ' (file_name,file_path,file_size,file_extension,uuid_hash,ts_run,  '
            ' ts_atime, ts_mtime, ts_ctime ) VALUES '
            ' (%(file_name)s, %(file_path)s, %(file_size)s, %(file_extension)s, '
            ' %(uuid_hash)s, %(ts_run)s, %(ts_atime)s,%(ts_mtime)s,%(ts_ctime)s) '
            ' returning id', 
            data
            )
        conn.commit()
        rows = cursor.fetchall()
        for row in rows:
            post_id = row[0]
    except Exception as e:
        ic('\n\n',type(e), e,'\n\n')
    finally:
        if conn:
            cursor.close()
            conn.close()
    return post_id

def update_directory_hash(file_path:str,uuid_hash:str):
    is_success = None
    try:
        conn = psycopg2.connect(static_connect_str)
        cursor = conn.cursor()
        cursor.execute(
            ' UPDATE dup_finder.directory ' 
            '   set uuid_hash = %(uuid_hash)s '
            '   where file_path = %(file_path)s '
            ,{
                'file_path':file_path,
                'uuid_hash':uuid_hash
            }   )
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

def process_dir(my_path,ts_run):
    file_data = {}
    dir_data = {}
    files_processed = 0
    bytes_processed = 0 
    total_files = 0
    if not check_dir_processed(my_path):
        for item in os.scandir(my_path):
            # faz a contagem de arquivos para barra de progresso
            if item.is_file: total_files += 1
        for item in tqdm(os.scandir(my_path),position=1,leave=None,desc="dir ..."+my_path[-40:],total=total_files):
            try:
                hd = get_file_hd(item.path)
                _, extension = os.path.splitext(item.path)
                root, name = os.path.split(item.path)
                assert name == item.name
                file_data.update({
                    'file_name':item.name,
                    'file_path':root,
                    'file_size' : item.stat().st_size,
                    'file_extension' : extension, 
                    'ts_atime': ts_to_dt(item.stat().st_atime),
                    'ts_mtime': ts_to_dt(item.stat().st_mtime),
                    'ts_ctime': ts_to_dt(item.stat().st_ctime),
                    'uuid_hash' : str(hex_2_uuid(hd)), 
                    'ts_run' : ts_run,
                })
                record_file_data(file_data)
                files_processed += 1
                bytes_processed += item.stat().st_size
                # TODO: fazer gravação dos dados do diretorio ao processar
                # dir_data={
                #     'file_path':root,
                #     'files_count':files_processed,
                #     'files_size' :bytes_processed,
                #     'ts_run':ts_run
                # }
                # record_dir_data(dir_data)
            except IsADirectoryError as e:
                if e.strerror != 'Is a directory':
                    print(e.strerror)
    return files_processed, bytes_processed

def get_directory_uuid_hash(root):
    """ 
    calcula o hash do diretório, busca os hashs dos files no directory
    atenção para ordem, pois afeta o resultado final
    """
    dir_hash = md5()
    try:
        conn = psycopg2.connect(static_connect_str)
        cursor = conn.cursor()
        cursor.execute(
            ' select uuid_hash from dup_finder.file f '
            ' where f.file_path  = %(root)s ' 
            '	order by f.uuid_hash, f.file_name '
            ,   {'root':root} 
        ) 
        conn.commit()
        rows = cursor.fetchall()
        for row in rows:
            file_hash = str(row[0])
            dir_hash.update(file_hash.encode('utf_8'))
    except Exception as e:
        print(type(e), e)
    finally:
        if conn:
            cursor.close()
            conn.close()

    return str(hex_2_uuid(dir_hash.hexdigest()))

def main():
    try:
        args = sys.argv
        base_path = args[1]
        ts_run = dt.now()
        t = Timer()
        t.start()
        files_processed = 0 
        bytes_processed = 0
        dirs_processed = 0

        total_dirs = 0
        for _, _, _ in os.walk(base_path):
            total_dirs +=1

        for root, dirs, files in tqdm(os.walk(base_path),total=total_dirs,
            position=0, leave=None, desc='loop diretórios'):
            dirs_processed += 1
            fls, bts = process_dir(root,ts_run)
            files_processed += fls
            bytes_processed += bts
    except KeyboardInterrupt:
        print('\n\nPROCESS INTERRUPTED BY USER\n\n')        
    finally:
        print(f'processed {files_processed} files and {dirs_processed} directories '
        f'total of {sizeof_fmt(bytes_processed)} \nin {t.elapsed()}')

if __name__ == '__main__': main()

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

def get_rows(sql):
    """wrapper para buscar registros, espera-se que a consulta
    seja do tipo SELECT retornando um ou mais registros """
    try:
        conn = psycopg2.connect(static_connect_str)
        cursor = conn.cursor()
        cursor.execute(sql) 
        conn.commit()
        rows = cursor.fetchall()
    finally:
        if conn:
            cursor.close()
            conn.close()

    return rows    

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

def hex_2_uuid(hd):
    return UUID(hd)

def get_file_hd(file,chunksize=128*10000):
    """calcula a hex digest do arquivo, le toda a info 
    para calcular o md5
    """
    try:
        with open(file, "rb") as f:
            file_hash = md5()
            while chunk := f.read(chunksize):
                file_hash.update(chunk)
    except (IsADirectoryError,FileNotFoundError):
        raise
    except Exception as e:
        ic(f'ocorreu erro imprevisto {e}')
        raise
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
    """ checa de diretorio ja foi processado, importante para quando 
    process é interrompido, não reprocessar hashes de arquivos
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

def process_dir_hashes():
    """ processa todos os diretórios
    que não tiveram hash ainda processado
    """
    n = 0
    try:
        conn = psycopg2.connect(static_connect_str)
        cursor = conn.cursor()
        cursor.execute('select file_path from dup_finder.directory d where d.uuid_hash is null') 
        conn.commit()
        rows = cursor.fetchall()

        for row in tqdm(rows,position=0,leave=False,
        desc=f'hashing directories',total=cursor.rowcount):
            file_path = row[0]
            uuid_hash = get_directory_uuid_hash(file_path)
            update_directory_hash(file_path,uuid_hash)
            n += 1

    except Exception as e:
        print(type(e), e)
    finally:
        if conn:
            cursor.close()
            conn.close()
    return n

def record_dir_data(data:dict):
    # TODO: construir, colocar dentro de process_dir
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
        ic(e)
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

def process_dir(my_path,ts_run,reprocess_dirs=False):
    file_data = {}
    dir_data = {}
    files_processed = 0
    bytes_processed = 0 
    total_files = 0
    if reprocess_dirs: dir_is_processed = False
    else: dir_is_processed = check_dir_processed(my_path)
    ic(dir_is_processed)
    if not dir_is_processed:
        ic()
        for item in os.scandir(my_path):
            ic()
            # faz a contagem de arquivos para barra de progresso
            if item.is_file: total_files += 1
        for item in tqdm(os.scandir(my_path),position=1,
        leave=None,desc="dir ..."+my_path[-40:],total=total_files):
            try:
                assert item.is_file()
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
            except AssertionError:
                pass
            # except IsADirectoryError as e:
            #     if e.strerror != 'Is a directory':
            #         print(e.strerror)
            # except FileNotFoundError as e:
            #     if e.strerror != 'No such file or directory':
            #         print(e.strerror)
    return files_processed, bytes_processed

def get_duplicate_dirs_hash():
    hashes = []

    sql = """select
		uuid_hash ,
		min(total_bytes) as filse_size,
		count(1) duplicates_count,
		count(1) * min(total_bytes) as total_size,
		sum(d.files_count) / count(1) as files_per_folder, 
		(count(1)-1) * min(total_bytes) as saveable_space 
	from dup_finder.directory d -- select * from directory
	group by uuid_hash 
	having count(1) > 1  
	order by (count(1)-1) * min(total_bytes) desc
    """

    try:
        conn = psycopg2.connect(static_connect_str)
        cursor = conn.cursor()
        cursor.execute(sql) 
        conn.commit()
        rows = cursor.fetchall()
        for row in rows:
            hashes.append(row[0])
    finally:
        if conn:
            cursor.close()
            conn.close()

    return hashes

def get_paths_for_hash(hash):

    sql = f"""
    select file_path 
    from dup_finder.directory d 
    where d.uuid_hash = '{hash}'
    """
    paths = {}
    n = 0 
    try:
        conn = psycopg2.connect(static_connect_str)
        cursor = conn.cursor()
        cursor.execute(sql) 
        conn.commit()
        rows = cursor.fetchall()
        for row in rows:
            paths.update({n:row[0]})
            n += 1
    finally:
        if conn:
            cursor.close()
            conn.close()

    return paths

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

def insert_new_directories():
    """ funcao executa sql que inclui novos diretorios
    que ainda não estejam no identificados
    """
    sql = """
        insert into dup_finder.directory 
        select 
                f.file_path, 
                sum(f.file_size) as total_bytes, 
                count(1) as files_count, 
                null::uuid uuid_hash
            from dup_finder.file f 
                left join dup_finder.directory d on 
                    f.file_path = d.file_path
            where d.file_path is null
            group by f.file_path 
    """
    is_success = exec_command(sql)
    return False if is_success is None else is_success    

def main(base_path,ts_run=dt.now(),reprocess_dirs=False):
    base_path = os.path.abspath(base_path)
    t = Timer()
    t.start()
    files_processed = 0 
    bytes_processed = 0
    dirs_processed = 0
    try:
        total_dirs = 0
        for _, _, _ in os.walk(base_path):
            total_dirs +=1

        for root, dirs, files in tqdm(os.walk(base_path),total=total_dirs,
        position=0, leave=None, desc='loop diretórios'):
            dirs_processed += 1
            fls, bts = process_dir(root,ts_run,reprocess_dirs=reprocess_dirs)
            files_processed += fls
            bytes_processed += bts
    except KeyboardInterrupt:
        print('\n\nPROCESS INTERRUPTED BY USER\n\n')
    except Exception as e:
        raise e    
    finally:
        print(f'processed {files_processed} files and {dirs_processed} directories '
        f'total of {sizeof_fmt(bytes_processed)} \nin {t.elapsed()}')

    if insert_new_directories(): print(f'new directories inserted')

    if process_dir_hashes() > 0: print(f'hashes de diretorios calculados    ')

    print('finished processing')

if __name__ == '__main__': 
    try:
        ### processamento de argumentos
        ### 1 > caminho
        ### 2 > debug
        args = sys.argv
        base_path = args[1]
        debug = False
        reprocess_dirs = False
        for arg in args:
            # try:
            if arg[0:6] == '-debug': debug = True
            elif arg[0:15] == '-reprocess_dirs': reprocess_dirs = True
        ic.enable() if debug else ic.disable()
        ic(args)
        base_path = os.path.abspath(base_path)
        assert os.path.isdir(base_path)
        ic(base_path, debug, reprocess_dirs)    
        print(f'iniciando processamento de {base_path}')
        main(base_path,reprocess_dirs=reprocess_dirs)
    except AssertionError:
        print(f'caminho {args[1]} não encontrado\n')
    

-- status do BD
select ts_run, count(1) total_files_processed, 
		sum(file_size) total_bytes_processed, 
		max(ts_created),
		count(distinct file_path) as total_directories,
		count(distinct file_extension) as distinct_extensions,
		ts_run::text txt_ts
	from file f
	group by ts_run 
	order by ts_run desc
;

-- query para consultar lag - tempo de processamento dos arquivos
-- detecta "pausas" de processamento, ou tempo médio para processar MBs  / s
select 
		f.id, 
		f.ts_created , 
		lag(ts_created , 1) over (order by f.ts_run, f.ts_created),
		f.ts_created - lag(ts_created , 1) over (order by f.ts_run, f.ts_created) as time_delta,
		f.file_size ,
		f.uuid_hash 
	from file f 
order by (f.ts_created - lag(ts_created , 1) over (order by f.ts_run, f.ts_created) ) desc
;

-- query para comparar diretórios semelhantes
-- para criar tabela base
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
	order by sum(f.file_size) desc
;

select * from dup_finder.directory

-- query para trazer dados dos arquivos no diretório para computar hash do diretório
-- importante a ordenação dos arquivos pelo hash
select uuid_hash from file f 
	where f.file_path  = '/media/mscalabrin/My Passport/Nossas Fotos/2013 Photos/2012/2012 Diversos'
	order by f.uuid_hash, f.file_name
;

-- query para buscar dados de uma rodada
select distinct ts_run::text from dup_finder.file f2 order by 1 desc ;
select * 
	from dup_finder.file f 
	where ts_run =  '2021-03-18 12:22:47.792984'
;

-- CUIDADO! deleta!	
--delete from dup_finder.file f
--	where ts_run = '2021-03-28 22:37:02.370315'
;


-- testando se path já foi encontrado
select '/media/mscalabrin/My Passport/mscala1/Documents/Scalabrin Docs/IP' in (select distinct file_path from file)

-- detecção de arquivos duplicados
select
		uuid_hash ,
		min(file_size) as filse_size,
		count(1) duplicates_count,
		count(1) * min(file_size) as total_size,
		(count(1)-1) * min(file_size) as saveable_space 
	from file f 
		where lower(f.file_extension) in ('.jpg','.tiff','.jpge','.pdf') 
	group by uuid_hash 
	having count(1) > 1
--	order by count(1) * min(file_size) desc
--	order by uuid_hash 
	order by (count(1)-1) * min(file_size) desc -- ordenando por espaço economizavel
--	order by count(1) desc -- ordenando por repetições
;	

select * from file f2 where f2.uuid_hash = '1a620c92-51cf-c606-1faa-67c516b5c1c9'
;


select
		file_extension ,
		min(file_size) as size,
		count(1),
		count(1) * min(file_size) as total_size
	from file f 
	group by file_extension 
--	order by count(1) * min(file_size) desc
--	order by uuid_hash 
	order by count(1) desc
;	

-- backup dos dados de file
select * into dup_finder.file_bak from dup_finder.file;

-- restaura backup do file




-- /media/mscalabrin/My Passport/mscala1/
update dup_finder.directory
	set uuid_hash = null--'55e319ec-4c0b-21f5-4fc9-655af09d5ece'
	where file_path = '/media/mscalabrin/My Passport/mscala1/Dropbox'
;

select * from directory where file_path = '/media/mscalabrin/My Passport/mscala1/Dropbox'


-- query para buscar diretorios nao processados
select file_path from dup_finder.directory d where d.uuid_hash is null
;

-- query para mostrar diretórios duplicados
select
		uuid_hash ,
		min(total_bytes) as filse_size,
		count(1) duplicates_count,
		count(1) * min(total_bytes) as total_size,
		sum(d.files_count) / count(1) as files_per_folder, 
		(count(1)-1) * min(total_bytes) as saveable_space 
	from dup_finder.directory d -- select * from directory
	group by uuid_hash 
	having count(1) > 1  
	order by (count(1)-1) * min(total_bytes) desc -- ordenando por espaço economizavel
	  -- sum(d.files_count) / count(1) desc para ordenar por qtd de arquivos
;


select file_path from dup_finder.directory d where d.uuid_hash = 'a1f31e53-afee-6249-5eb1-71e100207f2e'

select * from directory d where d.uuid_hash = '2b7156d7-f046-a8ee-cf44-8f686144359e'

seleciona 

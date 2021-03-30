/*
 * sql para inserir novos diretórios
 * 
 */

--insert into dup_finder.directory 
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
;


delete from dup_finder.directory where uuid_hash is null;



select unnest(array[0,1,2]);




select d.ts_created::text, count(1) as directories
	from dup_finder.directory d
	group by d.ts_created 
	order by d.ts_created desc

select f.ts_run::text, count(1) as files
	from dup_finder.file f
	group by f.ts_run 
	order by f.ts_run desc
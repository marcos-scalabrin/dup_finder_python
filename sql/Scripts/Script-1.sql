/*
 * sql para inserir novos diretórios
 * 
 */

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
;


delete from dup_finder.directory where uuid_hash is null;
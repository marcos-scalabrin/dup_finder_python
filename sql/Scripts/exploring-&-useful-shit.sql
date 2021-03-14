select ts_run, count(1), sum(file_size), max(ts_created)
	from file f
	group by ts_run 
	order by ts_run desc
;


select * 
	from dup_finder.file f 
	where ts_run between timestamp '2021-03-14 01:54:34' - interval '1 second' and timestamp '2021-03-14 01:54:34' + interval '1 second' 

	
-- testando se path já foi encontrado
select '/media/mscalabrin/My Passport/mscala1/Documents/Scalabrin Docs/IP' in (select distinct file_path from file)

select
		uuid_hash ,
		min(file_size) as size,
		count(1),
		count(1) * min(file_size) as total_size
	from file f 
	group by uuid_hash 
	having count(1) > 1
	order by count(1) * min(file_size) desc
;	

select * from file f2 where f2.uuid_hash = 'bd7754c2-92dc-31c7-f830-e88c9349ee2f'

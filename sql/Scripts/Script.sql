select ts_run, count(1) 
	from file f
	group by ts_run 
	order by ts_run desc
;

select
		uuid_hash ,
		count(1)
	from file f 
	group by uuid_hash 
	order by count(1) desc
;	

select * 
into dup_finder.delete_file
from dup_finder.file limit 1

insert into dup_finder.delete_file select * from dup_finder.file limit 10

delete from dup_finder.delete_file 

select distinct extract(MICROSECONDS from ts_run), ts_run from dup_finder.file

select * from dup_finder.file f where ts_run = '2021-03-28 22:37:02.370315'
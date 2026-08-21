SELECT 1 AS T INTO #TEMP

select null is null from #temp

SELECT CASE WHEN NULL = NULL THEN 1 ELSE 0 END FROM #TEMP


select 1 as id , 'aa' as name
 into #temp1
union
select 2 as id , 'ab' as name
union
select 3 as id , 'ac' as name
union
select 4 as id , 'ad' as name
union
select 5 as id , 'ae' as name


drop table #temp2
select  1 id , 'xx' as eventname , 2 winner_id 
into #temp2
union
select  2 id , 'xb' as eventname , 3 winner_id 
union
select  3 id , 'xc' as eventname , 2 winner_id 
union
select  4 id , 'xd' as eventname , null winner_id 
union
select  5 id , 'xd' as eventname , null winner_id 


SELECT * FROM #temp1 WHERE id NOT IN (SELECT winner_id FROM #temp2)


select sum(1) from #temp1

select count(10) from #temp1
select sum(id) from #temp1

select sum(2) from #temp1
select sum(3) from #temp1

SELECT 8 from #temp1 order by id DESC LIMIT 2,1


select 23.455 as t into #temp3

select t , FLOOR(t),( t- FLOOR(t)) * 1000, CEILING(t)
from #temp3

---------- Swap two columns
select *, ROW_NUMBER() over (order by id) %2 as i, ROW_NUMBER() over (order by id) as rnk
into #te11
from #temp1

select *
 from #te11 a

select *
 from #te11 a
 left join #te11 b
	on a.rnk= b.rnk -1 and a.i = b.i + 1
 left join #te11 c
	on a.rnk= c.rnk +1 and a.i = c.i - 1


SELECT DISTINCT STUFF((
		select '; '+ name
		 from #temp1 I
		 --WHERE O.NAME = I.NAME
		 for xml path ('')
		 ),1,2,'')
FROM #TEMP1 O

SELECT STUFF( (SELECT '; '+ name from #temp1 I  for xml path ('')), 1,2, '')


select 29.34, round(29.34,0), floor(29.34), ceiling(29.34)


select * from #TEMP


select top 100 percent *, PERCENT_RANK() over (order by id) * 100 from #temp1

select top 81 percent *, PERCENT_RANK() over (order by id) * 100 from #temp1


select * from #temp1

select * from #temp2

--select * from #temp1 a
--inner join #temp2 b on a.id = b.winner_id


select * from #temp1 a
full  join #temp2 b on a.id = b.winner_id
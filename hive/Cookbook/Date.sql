select hiredate -interval 5 day as hd_minus_5D
       hiredate -interval 5 year as hd_min_5Y
    from emp
  where deptno=10

select datediff(day, allen_hd, ward_hd)
    from (
select hiredate as ward_hd
      from emp
   where ename = 'WARD'
         ) x, 
         (
select hiredate as allen_hd
    from emp
where ename = 'ALLEN'
          ) y













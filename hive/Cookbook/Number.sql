
# Average

select avg(sal) as avf_sal 
    from emp

select deptno, avg(sal) as avg_sal
   from emp 
   group by deptno

select max(sal) as max_sal , min(sal) as min_sal
   from emp

select deptno, max(sal) as max_sal, min(sal) as min_sal
   from emp
   groupby deptno

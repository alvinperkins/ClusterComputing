
# Average

select avg(sal) as avf_sal 
    from emp

select deptno, avg(sal) as avg_sal
   from emp 
   group by deptno



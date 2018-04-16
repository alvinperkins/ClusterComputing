select hiredate -interval 5 day as hd_minus_5D
       hiredate -interval 5 year as hd_min_5Y
    from emp
  where deptno=10


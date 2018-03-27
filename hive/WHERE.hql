

hive> SELECT * FROM employees
WHERE country = 'US' AND state = 'CA';

hive> SELECT name, salary, deductions["Federal Taxes"],
    > salary * (1 - deductions["Federal Taxes"])
    > FROM employees
    > WHERE round(salary * (1 - deductions["Federal Taxes"])) > 70000;

hive> SELECT name, salary, deductions["Federal Taxes"],
    > salary * (1 - deductions["Federal Taxes"]) as salary_minus_fed_taxes
    > FROM employees
    > WHERE round(salary_minus_fed_taxes) > 70000;

hive> SELECT e.* FROM
    > (SELECT name, salary, deductions["Federal Taxes"] as ded,
    > salary * (1 - deductions["Federal Taxes"]) as salary_minus_fed_taxes
    > FROM employees) e
    > WHERE round(e.salary_minus_fed_taxes) > 70000;

Predicate Operator

hive> SELECT name, salary, deductions['Federal Taxes']
    > FROM employees WHERE deductions['Federal Taxes'] > 0.2;

hive> SELECT name, salary, deductions['Federal Taxes'] FROM employees
    > WHERE deductions['Federal Taxes'] > cast(0.2 AS FLOAT);

hive> SELECT name, address.street FROM employees WHERE address.street LIKE '%Ave.';

hive> SELECT name, address.city FROM employees WHERE address.city LIKE 'O%';

hive> SELECT name, address.street FROM employees WHERE address.street LIKE '%Chi%';

hive> SELECT name, address.street
> FROM employees WHERE address.street RLIKE '.*(Chicago|Ontario).*';

hive> SELECT name, address FROM employees
WHERE address.street LIKE '%Chicago%' OR address.street LIKE '%Ontario%';





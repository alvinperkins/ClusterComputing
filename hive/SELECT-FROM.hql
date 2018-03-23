
CREATE TABLE employees (
  name         STRING,
  salary       FLOAT,
  subordinates ARRAY<STRING>,
  deductions   MAP<STRING, FLOAT>,
  address      STRUCT<street:STRING, city:STRING, state:STRING, zip:INT>
)
PARTITIONED BY (country STRING, state STRING);

hive> SELECT name, salary FROM employees;

hive> SELECT name, deductions["State Taxes"] FROM employees;

Specify Columns with Regular Expressions
hive> SELECT symbol, `price.*` FROM stockes;

computing with column values
hive> SELECT upper(name), salary, deductions["Federal Taxes"],
    > round(salary * (1 - deductions["Federal Taxes"])) FROM employees;

Aggregate functions
hive> SELECT count(*), avg(salary) FROM employees;

hive> SELECT upper(name), salary, deductions["Federal Taxes"],
    > round(salary * (1 - deductions["Federal Taxes"])) FROM employees
    > LIMIT 2;

hive> SELECT upper(name), salary, deductions["Federal Taxes"] as fed_taxes,
    > round(salary * (1 - deductions["Federal Taxes"])) as salary_minus_fed_taxes
    > FROM employees LIMIT 2;

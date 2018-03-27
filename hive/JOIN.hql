
Inner JOIN

hive> SELECT a.ymd, a.price_close, b.price_close
    > FROM stocks a JOIN stocks b ON a.ymd = b.ymd
    > WHERE a.symbol = 'AAPL' AND b.symbol = 'IBM';

CREATE EXTERNAL TABLE IF NOT EXISTS dividends (
    ymd
    STRING,
    dividend
    FLOAT
)
PARTITIONED BY (exchange STRING, symbol STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

hive> SELECT s.ymd, s.symbol, s.price_close, d.dividend
    > FROM stocks s JOIN dividends d ON s.ymd = d.ymd AND s.symbol = d.symbol
    > WHERE s.symbol = 'AAPL';

hive> SELECT a.ymd, a.price_close, b.price_close , c.price_close
    > FROM stocks a JOIN stocks b ON a.ymd = b.ymd
    > JOIN stocks c ON a.ymd = c.ymd
    > WHERE a.symbol = 'AAPL' AND b.symbol = 'IBM' AND c.symbol = 'GE';

JOIN Optimize
SELECT s.ymd, s.symbol, s.price_close, d.dividend
FROM dividends d JOIN stocks s ON s.ymd = d.ymd AND s.symbol = d.symbol
WHERE s.symbol = 'AAPL';

Left OUTER JOIN
hive> SELECT s.ymd, s.symbol, s.price_close, d.dividend
    > FROM stocks s LEFT OUTER JOIN dividends d ON s.ymd = d.ymd AND s.symbol = d.symbol
    > WHERE s.symbol = 'AAPL';

RIGHT OUTER JOIN

FULL OUTER JOIN

LEFT SEMI-JOIN

Cartesian Product JOINs

Map-side Joins






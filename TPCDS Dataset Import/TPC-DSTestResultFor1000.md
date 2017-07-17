# TPC-DS数据量

<img src="https://github.com/jimmy-src/img/blob/master/tpcds/tpcdscount.jpg" width = "" height = "" alt="图片名称" align=center />

# kudu/impala参数调整

1.	Kudu：wal日志和数据存放日志分开，wal放在第一块磁盘上，如果有条件可以放在ssd上，数据目录放在第二块到第十二块上面

<img src="https://github.com/jimmy-src/img/blob/master/kudu/kudustorage.jpg" width = "" height = "" alt="图片名称" align=center />

2.	Kudu：memory_limit_hard_bytes参数调整为32G，默认是4G，block_cache_capacity_mb参数调整为4G，默认是512M

<img src="https://github.com/jimmy-src/img/blob/master/kudu/kudumem.jpg" width = "" height = "" alt="图片名称" align=center />

3. impala内存调整,catalog调整到32G，limit调到64G

<img src="https://github.com/jimmy-src/img/blob/master/impala/impalamem.jpg" width = "" height = "" alt="图片名称" align=center />

# 精准查询测试

表名  |      行数         | count查询
------------------- | -----------   |--------
call_center           |42       |8.11s
catalog_page          |30000    |3.03s
catalog_returns       |143996756 |5.35s
catalog_sales         |1439980416 |5.29s
customer              |12000000 |4.09s
customer_address      |6000000  |3.32s
customer_demographics |1920800  |3.98s
date_dim              |73049    |3.88s
household_demographics|7200     |4.00s
income_band           |20       |3.98s
inventory             |783000000 |5.42s
item                  |300000   |4.55s
promotion             |1500     |3.97s
reason                |65       |4.00s
ship_mode             |20       |3.98s
store                 |1002     |3.02s
store_returns         |287999764 |7.76s
store_sales           |2879987999 |9.19s
time_dim              |86400    |3.98s
warehouse             |20       |3.98s
web_page              |3000     |3.99s
web_returns           |71997522 |4.09s
web_sales             |720000376 |5.25s
web_site              |54       |4.64s

测试结果比较奇怪，有些数据量几十条的count时间却比较长

# TPC-DS 99条SQL的测试


查询SQL   |   查询类别    |   返回行数/查询时间    |   返回样例行
--------------------|----------------------|----------------------------|---------------------
query12.sql     |				|Fetched 100 row(s) in 24.89s				|
query13.sql     |				|Fetched 1 row(s) in 244.15s				|
query15.sql     |				|Fetched 100 row(s) in 155.48s				|
query17.sql     |				|Fetched 100 row(s) in 635.77s				|
query18.sql     |				|				|
query19.sql     |				|Fetched 100 row(s) in 1275.33s				|
query20.sql     |				|Fetched 100 row(s) in 19.39s				|
query21.sql     |				|				|
query22.sql     |				|				|
query24.sql     |				|				|
query25.sql     |				|Fetched 100 row(s) in 682.62s				|
query26.sql     |				|Fetched 100 row(s) in 35.03s				|
query27.sql     |				|Fetched 100 row(s) in 73.53s				|
query28.sql     |				|Fetched 1 row(s) in 91.90s				|
query29.sql     |				|Fetched 100 row(s) in 622.50s				|
query31.sql     |				|Fetched 261 row(s) in 96.36s				|
query32.sql     |				|Fetched 1 row(s) in 19.44s				|
query34.sql     |				|Fetched 47226 row(s) in 43.59s				|
query39.sql     |				|Fetched 9948 row(s) in 22.30s				|
query3.sql      |				|Fetched 100 row(s) in 750.28s				|
query40.sql     |				|				|
query42.sql     |				|Fetched 12 row(s) in 684.20s			|
query43.sql     |				|Fetched 100 row(s) in 736.33s					|
query45.sql     |				|Fetched 100 row(s) in 62.53s				|
query46.sql     |				|Fetched 100 row(s) in 93.57s				|
query48.sql     |				|Fetched 1 row(s) in 62.45s				|
query49.sql     |				|Fetched 803 row(s) in 146.91s				|
query50.sql     |				|Fetched 100 row(s) in 182.65s				|
query51.sql     |				|Fetched 100 row(s) in 125.54s				|
query52.sql     |				|Fetched 100 row(s) in 638.14s				|
query54.sql     |				|Fetched 0 row(s) in 1422.45s				|
query55.sql     |				|Fetched 100 row(s) in 625.70s				|
query56.sql     |				|Fetched 100 row(s) in 65.93s				|
query58.sql     |				|Fetched 57 row(s) in 55.40s				|
query60.sql     |				|Fetched 100 row(s) in 35.36s				|
query63.sql     |				|Fetched 100 row(s) in 1095.85s				|
query64.sql     |				|Fetched 7094 row(s) in 330.30s				|
query65.sql     |				|Fetched 100 row(s) in 83.53s				|
query66.sql     |				|Fetched 20 row(s) in 51.78s				|
query67.sql     |				|				|
query68.sql     |				|Fetched 0 row(s) in 93.49s				|
query70.sql     |				|				|
query71.sql     |				|Fetched 339134 row(s) in 107.12s				|
query72.sql     |				|				|
query73.sql     |				|Fetched 270 row(s) in 29.39s				|
query75.sql     |				|Fetched 100 row(s) in 182.52s				|
query76.sql     |				|Fetched 100 row(s) in 30.04s				|
query79.sql     |				|Fetched 100 row(s) in 65.41s				|
query7.sql      |				|Fetched 100 row(s) in 41.88s				|
query80.sql     |				|				|
query82.sql     |				|Fetched 23 row(s) in 458.93s				|
query83.sql     |				|Fetched 100 row(s) in 13.73s				|
query84.sql     |				|Fetched 100 row(s) in 66.44s				|
query85.sql     |				|Fetched 64 row(s) in 65.24s				|
query87.sql     |				|Fetched 1 row(s) in 134.65s				|
query88.sql     |				|Fetched 1 row(s) in 61.48s				|
query89.sql     |				|Fetched 100 row(s) in 1202.93s				|
query90.sql     |				|Fetched 1 row(s) in 9.02s				|
query91.sql     |				|Fetched 42 row(s) in 42.49s				|
query92.sql     |				|Fetched 1 row(s) in 100.90s				|
query93.sql     |				|Fetched 100 row(s) in 202.53s				|
query94.sql     |				|Fetched 1 row(s) in 4760.46s				|
query95.sql     |				|Fetched 1 row(s) in 16701.69s				|
query96.sql     |				|Fetched 1 row(s) in 44.87s				|
query97.sql     |				|Fetched 1 row(s) in 95.42s				|
query98.sql		|				|				|	


执行过程记录参考：TPC-DS sample sql result.md
















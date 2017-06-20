### 建表分区以及数据导入

> 建表分区这一块没有去细细研究，tpch-ds有99条sql也没测试过，没具体研究怎么分区能让性能更强,有研究的同学请不吝赐教
> 数据导入的话这里调用的是sqlcontext从hive里面读取数据，tpch-ds生成的数据集默认已经加载到hive了，以orc格式存储，注意impala不支持orc格式，所以在impala里面查询会报错

1. customer
```
CREATE TABLE `customer`(
  `c_custkey` bigint,
  `c_name` string,
  `c_address` string,
  `c_nationkey` bigint,
  `c_phone` string,
  `c_acctbal` double,
  `c_mktsegment` string,
  `c_comment` string,
  PRIMARY KEY(c_custkey))
PARTITION BY HASH PARTITIONS 10
STORED AS KUDU;
```
COUNT:15000000
```
val acTransDF = sqlContext.table("tpch_flat_orc_100.customer")
val kuduContext = new KuduContext("zjdw-pre0069:7051")
kuduContext.tableExists("impala::default.customer")
acTransDF.write.options(Map("kudu.master"-> "zjdw-pre0069:7051", "kudu.table"-> "impala::default.customer")).mode("append").kudu
```

2. lineitem
```
CREATE TABLE `lineitem`(
  `l_orderkey` bigint,
  `l_partkey` bigint,
  `l_suppkey` bigint,
  `l_linenumber` int,
  `l_quantity` double,
  `l_extendedprice` double,
  `l_discount` double,
  `l_tax` double,
  `l_returnflag` string,
  `l_linestatus` string,
  `l_shipdate` string,
  `l_commitdate` string,
  `l_receiptdate` string,
  `l_shipinstruct` string,
  `l_shipmode` string,
  `l_comment` string,
  PRIMARY KEY(l_orderkey,l_partkey,l_suppkey))
PARTITION BY HASH (l_orderkey) PARTITIONS 8,
             HASH (l_partkey) PARTITIONS 8
STORED AS KUDU;
```

COUNT:600037902

```
val acTransDF = sqlContext.table("tpch_flat_orc_100.lineitem")
val kuduContext = new KuduContext("zjdw-pre0069:7051")
kuduContext.tableExists("impala::default.lineitem")
acTransDF.write.options(Map("kudu.master"-> "zjdw-pre0069:7051", "kudu.table"-> "impala::default.lineitem")).mode("append").kudu
```


3. nation
```
CREATE TABLE `nation`(
  `n_nationkey` bigint,
  `n_name` string,
  `n_regionkey` bigint,
  `n_comment` string,
    PRIMARY KEY(n_nationkey))
PARTITION BY HASH PARTITIONS 2
STORED AS KUDU;
```

COUNT:25
```
val acTransDF = sqlContext.table("tpch_flat_orc_100.nation")
val kuduContext = new KuduContext("zjdw-pre0069:7051")
kuduContext.tableExists("impala::default.nation")
acTransDF.write.options(Map("kudu.master"-> "zjdw-pre0069:7051", "kudu.table"-> "impala::default.nation")).mode("append").kudu
```
4. orders
```
CREATE TABLE `orders`(
  `o_orderkey` bigint,
  `o_custkey` bigint,
  `o_orderstatus` string,
  `o_totalprice` double,
  `o_orderdate` string,
  `o_orderpriority` string,
  `o_clerk` string,
  `o_shippriority` int,
  `o_comment` string,
  PRIMARY KEY(o_orderkey,o_custkey))
PARTITION BY HASH (o_orderkey) PARTITIONS 8,
             HASH (o_custkey) PARTITIONS 8
STORED AS KUDU;
```

COUNT:150000000
```
val acTransDF = sqlContext.table("tpch_flat_orc_100.orders")
val kuduContext = new KuduContext("zjdw-pre0069:7051")
kuduContext.tableExists("impala::default.orders")
acTransDF.write.options(Map("kudu.master"-> "zjdw-pre0069:7051", "kudu.table"-> "impala::default.orders")).mode("append").kudu
```

5. part
```
CREATE TABLE `part`(
  `p_partkey` bigint,
  `p_name` string,
  `p_mfgr` string,
  `p_brand` string,
  `p_type` string,
  `p_size` int,
  `p_container` string,
  `p_retailprice` double,
  `p_comment` string,
  PRIMARY KEY(p_partkey))
PARTITION BY HASH (p_partkey) PARTITIONS 10
STORED AS KUDU;
```

COUNT:150000000
```
val acTransDF = sqlContext.table("tpch_flat_orc_100.part")
val kuduContext = new KuduContext("zjdw-pre0069:7051")
kuduContext.tableExists("impala::default.part")
acTransDF.write.options(Map("kudu.master"-> "zjdw-pre0069:7051", "kudu.table"-> "impala::default.part")).mode("append").kudu
```

6. partsupp
```
CREATE TABLE `partsupp`(
  `ps_partkey` bigint,
  `ps_suppkey` bigint,
  `ps_availqty` int,
  `ps_supplycost` double,
  `ps_comment` string,
  PRIMARY KEY(ps_partkey,ps_suppkey))
PARTITION BY HASH (ps_partkey) PARTITIONS 5,
			 HASH (ps_suppkey) PARTITIONS 5
STORED AS KUDU;
```

COUNT:80000000
```
val acTransDF = sqlContext.table("tpch_flat_orc_100.partsupp")
val kuduContext = new KuduContext("zjdw-pre0069:7051")
kuduContext.tableExists("impala::default.partsupp")
acTransDF.write.options(Map("kudu.master"-> "zjdw-pre0069:7051", "kudu.table"-> "impala::default.partsupp")).mode("append").kudu
```

7. region
```
CREATE TABLE `region`(
  `r_regionkey` bigint,
  `r_name` string,
  `r_comment` string,
    PRIMARY KEY(r_regionkey))
  PARTITION BY HASH PARTITIONS 2
STORED AS KUDU;
```

COUNT:5
```
val acTransDF = sqlContext.table("tpch_flat_orc_100.region")
val kuduContext = new KuduContext("zjdw-pre0069:7051")
kuduContext.tableExists("impala::default.region")
acTransDF.write.options(Map("kudu.master"-> "zjdw-pre0069:7051", "kudu.table"-> "impala::default.region")).mode("append").kudu
```

8. supplier
```
CREATE TABLE `supplier`(
  `s_suppkey` bigint,
  `s_name` string,
  `s_address` string,
  `s_nationkey` bigint,
  `s_phone` string,
  `s_acctbal` double,
  `s_comment` string,
  PRIMARY KEY(s_suppkey))
PARTITION BY HASH (s_suppkey) PARTITIONS 2
STORED AS KUDU;
```

COUNT:1000000
```
val acTransDF = sqlContext.table("tpch_flat_orc_100.supplier")
val kuduContext = new KuduContext("zjdw-pre0069:7051")
kuduContext.tableExists("impala::default.supplier")
acTransDF.write.options(Map("kudu.master"-> "zjdw-pre0069:7051", "kudu.table"-> "impala::default.supplier")).mode("append").kudu
```


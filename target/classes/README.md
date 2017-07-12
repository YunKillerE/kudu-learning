# 一，Study Materials 

一些学习资料以及ppt

# 二，TPCH Dataset

[数据生成工具](https://github.com/jimmy-src/kudu-learning/tree/master/TPC-DS%20tools%20TAR):见TPC-DS tools TAR目录hive-testbench.tgz

[tpcds数据生成](https://github.com/jimmy-src/kudu-learning/tree/master/TPCDS%20Dataset%20Import):见TPCDS Dataset Import目录

[数据导入kudu](https://github.com/jimmy-src/kudu-learning/tree/master/TPCDS%20Dataset%20Import): 见TPCDS Dataset Import目录

# 三，src/main scala代码

kudu和spark的集成代码，主要实现了增删改查(整理中...)

# 四，精准查询测试及OLAP测试

见TPC-DS Performance Result目录

# 五，spark-shell REPL 手动从hdfs上插入数据到kudu

>> 环境：CDH 5.8.4 KUDU 1.3.0

## 1，启动spark-shell
```
#注意这里会联网下载kudu的jar包，如果不能联网，可以去其他能联网的机器上执行下载好，再将/root/.ivy2/cache/org.apache.kudu/kudu-spark_2.10/目录打包过去放到相同位置就行了
#也可以直接去https://repo1.maven.org/maven2/org/apache/kudu下面选择对应版本的jar包下载下来，执行spark-shell --jars kudu-$verr
#如果环境和我一样的，可以直接使用TPCH-DS tools TAR/kudu-spark_2.10.tgz文件

spark-shell --packages org.apache.kudu:kudu-spark_2.10:1.3.0

import org.apache.kudu.spark.kudu._
import org.apache.kudu.client._
import collection.JavaConverters._
```

## 2，定义case class
```
//注意这里列名称要小写，之所以小写是因为在impala中创建的表的是小写，如果这里大写写入会无法识别列名称
case class Trans(id: Int, hjd_dzmc: String, xzd_dzmc: String, fwcs: String, asjxgrybh: String, xm: String, gmsfhm: String, xbdm: String, mzdm: String, hyzkdm: String, xldm: String, gxsj: String)
//我的原始数据里面列有单引号，这里去掉，否则会后面会报错
def toTrans = (trans: Seq[String]) => Trans(trans(0).replace("\'", "").trim.toInt,trans(1).replace("\'", ""),trans(2).replace("\'", ""),trans(3).replace("\'", ""),trans(4).replace("\'", ""),trans(5).replace("\'", ""),trans(6).replace("\'", ""),trans(7).replace("\'", ""),trans(8).replace("\'", ""),trans(9).replace("\'", ""),trans(10).replace("\'", ""),trans(11).replace("\'", ""))
```

## 3，从hdfs上读取数据，创建DF，如果在hive，也可以直接用sqlContext来创建DF，就可以不用创建case class
```
val acTransRDD = sc.textFile("hdfs://cmdata1:8020//kudu_spark/674581054918776").map(_.split(",")).map(toTrans(_))

//spark2.x
//val acTransDF = spark.createDataFrame(acTransRDD)
//hive
//val acTransDF = sqlContext.table("tpch_flat_orc_100.customer")
//spark 1.6
val acTransDF = sqlContext.createDataFrame(acTransRDD)
```

> 从kudu读取数据：val dk = spark.read.options(Map("kudu.master"-> "192.168.3.79:7051", "kudu.table"-> "impala::kudu_spark_tpcds_1000.catalog_returns")).kudu

## 4,创建kudu表（可选，也可以在impala里面先创建好）
```
//创建kuducontext，后面填写kudu master地址，leader一定要在列表里面
val kuduContext = new KuduContext("192.168.1.12:7051")
//检查检查表是否存在
kuduContext.tableExists("impala::default.yunchen_spark_table_test")
//建表，hash id，共建10个分区，拷贝份数为1，这里建的表无法在impala中显示，不知道为什么，如果要建表这建议调用impala的api进行创建表
kuduContext.createTable(
    "yunchen_spark_table_test", acTransDF.schema, Seq("id"),
    new CreateTableOptions()
        .setNumReplicas(1)
        .addHashPartitions(List("id").asJava, 10))
```

## 5，写入kudu中
```
//直接将df写入到kudu中
acTransDF.write.options(Map("kudu.master"-> "192.168.1.12:7051", "kudu.table"-> "impala::default.spark_kudu")).mode("append").kudu
```

读取见src/main

# 六，kudu的参数调整

刚开始导入15亿数据导论一天没完成，比较奇怪为什么写入性能这么差？后来发现cdh的默认配置比较坑，以前发现cdh yarn的默认配置也比较坑
    
这里从两个方面进行了优化：

* 1.wal日志和数据存放日志分开，wal放在第一块磁盘上，如果有条件可以放在ssd上，数据目录放在第二块到第十二块上面

* 2.memory_limit_hard_bytes参数调整为32G，默认是4G，block_cache_capacity_mb参数调整为4G，默认是512M

* 3.Impala:  Catalog Server设置内存为32gb；impalad内存设置为64gb

参数调整后，15亿数据大概6分钟就导入进去了，写入性能提升巨大









# kudu-learning
kudu学习的一些资料，以及和spark/impala的集成使用

# Study Materials 

一些学习资料以及ppt

# TPCDS Dataset Import

[数据生成工具]():见TPCDS tools TAR目录hive-testbench.tgz
[tpcds数据生成]():见TPCDS Dataset Import目录
[数据导入kudu](): 见TPCDS Dataset Import目录

# src/main scala代码

kudu和spark的集成代码，主要实现了增删改查(整理中...)

# spark-shell REPL 手动从hdfs上插入数据到kudu

>> 环境：CDH 5.8.4 KUDU 1.3.0

## 1，启动spark-shell
```
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

## 3，从hdfs上读取数据，创建DF
```
val acTransRDD = sc.textFile("hdfs://cmdata1:8020//kudu_spark/674581054918776").map(_.split(",")).map(toTrans(_))

//spark2.x
//val acTransDF = spark.createDataFrame(acTransRDD)
//spark 1.6
val acTransDF = sqlContext.createDataFrame(acTransRDD)
```

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
















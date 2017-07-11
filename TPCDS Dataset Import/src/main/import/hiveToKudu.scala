package `import`

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession

/**
  * Created by yunchen on 2017/7/4.
  *
  * 提交1：/opt/spark2/bin/spark-submit --jars kudu-spark2_2.11-1.3.0.jar --master yarn --queue root.public kudu-learning.jar --class import.hiveToKudu
  *
  *   如果按照上面提交，大批量数据（大于15亿行的时候）会出现timeout报错，所以改成如下了：
  *
  * 提交2：/opt/spark2/bin/spark-submit --jars kudu-spark2_2.11-1.3.0.jar --master yarn-client --num-executors 30 --executor-memory 8G --executor-cores 2 --conf spark.rpc.askTimeout=600s --conf spark.network.timeout=600s --queue root.public --class import.hiveToKudu kudu-learning.jar
  *
  */
object hiveToKudu {

  def main(args: Array[String]): Unit = {

    //定义kudu master地址，多个用逗号分隔
    val kuduMaster = "zjdw-pre0069:7051"
    //定义hive源数据库名
    val srcDatabaseName = "tpcds_bin_partitioned_parquet_1000"
    //定义hive源表名数组
   /* val arrTableName =  Array[String]("call_center","catalog_page","customer","customer_address","customer_demographics","date_dim","household_demographics"
            ,"income_band","inventory","item","promotion","reason","ship_mode","store","time_dim","warehouse","web_page","web_returns","web_site","catalog_returns"
            ,"store_returns","catalog_sales","web_sales","store_sales")*/
    /*val arrTableName =  Array[String]("customer","customer_address","customer_demographics",
      "date_dim","household_demographics","income_band","inventory","item","promotion","time_dim",
     "warehouse","web_page","reason","ship_mode","store","store_returns","catalog_sales","store_sales")*/
    val arrTableName = Array[String]("store_sales")
    //定义目标数据库名
    val dstDatabaseName = "kudu_spark_tpcds_1000"

    //定义sparksession
    val spark = SparkSession
      .builder()
      .appName("Import Hive Data TO Kudu Use Spark")
      .enableHiveSupport()
      .getOrCreate()

    for ( srcTableName <- arrTableName ) {
      println("当前写入的表是："+srcTableName)
      readHiveToDF(kuduMaster, dstDatabaseName, srcDatabaseName,srcTableName, spark)
    }

  }

  def readHiveToDF(kuduMaster:String, dstDatabaseName:String, srcDatabaseName:String, srcTableName:String, sp:org.apache.spark.sql.SparkSession) = {

    val dstTableName = srcTableName

    //val kuduContext = new KuduContext("zjdw-pre0069:7051")
    val kuduContext = new KuduContext(kuduMaster)

    import sp.implicits
    val tableDF = sp.read.table(srcDatabaseName+"."+srcTableName)

    ////这里支持insertRows，deleteRows，upsertRows，详细见官网实例：http://kudu.apache.org/docs/developing.html
    kuduContext.upsertRows(tableDF,"impala::"+dstDatabaseName+"."+dstTableName)

    //下面这种方式只支持appent模式,这里.kudu报错，查看过是有那个class的，里面的方法也有，不知道为什么，前面查询都可以
    //tableDF.write.options(Map("kudu.master"-> kuduMaster, "kudu.table"-> "impala::default.spark_kudu")).mode("append").kudu

  }

}







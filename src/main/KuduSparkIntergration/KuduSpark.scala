package main.KuduSparkIntergration

import org.apache.kudu.spark.kudu.{KuduContext, _}
import org.apache.spark.sql.SparkSession
import test.CreateTableUseSparkTest
/**
  * Created by yunchen on 2017/6/12.
  */
object KuduSpark {

  /**
    * kudu主函数，主要用于调用CRUD，根据传入的参数
    */

  def main(args: Array[String]): Unit = {

    println("hello kudu")
    val masterList = "cmname1:7051"
    val kuduContext = new KuduContext(masterList)

    //创建测试DF
    val spark = SparkSession
      .builder()
      .appName("kudu CRUD operation")
      .enableHiveSupport()
      .getOrCreate()

    //下面这种创建DF的写法不知道为什么一直不行
    /*
    //To be able to to use toDF you have to import sqlContext.implicits first

    val sc = spark.sparkContext
    case class Trans(s_suppkey:Int, s_address: String, s_nationkey:Int, s_phone: Double, s_acctbal: String, s_comment: String)

    /*
    * https://stackoverflow.com/questions/39433419/encoder-error-while-trying-to-map-dataframe-row-to-updated-row
    *
    * 如果不加下面这行会报错，加了之后就无法运行，提示unknown column: value
    * Error:(37, 72) Unable to find encoder for type stored in a Dataset.  Primitive types (Int, String, etc) and Product types (case classes) are supported by importing spark.implicits._  Support for serializing other types will be added in future releases.
    * val acTransDF1 = sc.parallelize(acTransList1).map(_.split(",")).map(toTrans(_)).toDF()
    *
    * */
    //implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Trans]

    def toTrans = (trans: Seq[String]) => Trans(trans(0).trim.toInt, trans(1),trans(2).trim.toInt,trans(3).trim.toDouble,trans(4),trans(5))
    val acTransList1 = Array("10001,ppppppp,1111,13999999999,fdsa,fdsa","10101,uuuuuuuu,222,13999998999,fdsa,fdsa")
    import spark.implicits._
    val acTransDF1 = sc.parallelize(acTransList1).map(_.split(",")).map(toTrans(_)).toDF()
    val acTransList2 = Array("10001,ddddddd,1111,13999999999,fdsa,fdsa")
    val acTransDF2 = sc.parallelize(acTransList2).map(_.split(",")).map(toTrans(_)).toDF()
    val acTransList3 = Array("10001,wwwwwwwww,2222,13999999999,fdsa,fdsa")
    val acTransDF3 = sc.parallelize(acTransList3).map(_.split(",")).map(toTrans(_)).toDF()*/

    /*
    * Exception in thread "main" java.lang.NoSuchMethodError: scala.reflect.api.JavaUniverse.runtimeMirror(Ljava/lang/ClassLoader;)Lscala/reflect/api/JavaMirrors$JavaMirror;
    *
    * 本机scala版本要与集群版本一致，如果是spark2，一定要是scala2.11
    * */

    import spark.implicits._

    val acTransDF1 = Seq(
      (10001,"ppppppp",1111,13999999999D,"fdsa","fdsa"), (10101,"ppppppp",222,13999989999D,"fdsa","fdsa")
    ).toDF("s_suppkey", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment")

    val acTransDF2 = Seq(
      (10001,"wwwwwww",3333,13999999999D,"fdsa","fdsa")
    ).toDF("s_suppkey", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment")

    val acTransDF3 = Seq(
      (10001,"uuuuuuuu",5555555,13999999999D,"fdsa","fdsa")
    ).toDF("s_suppkey", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment")

    val kuduTableName = "spark_yunchen_supple"

    val ct = new CreateTableUseSparkTest()

    //创建表，这里创建的表在impala里面看不到
    ct.CreateTableUseSpark(kuduContext)
    println("创建表，如果存在则删除.....")

    //查询数据
    val df = spark.read.options(Map("kudu.master" -> "192.168.3.79:7051","kudu.table" -> kuduTableName)).kudu
    println("没有数据结果：")
    df.show()

    //插入数据
    acTransDF2.collect().foreach(println)
    kuduContext.insertRows(acTransDF2,kuduTableName)
    println("插入数据后的结果：")
    df.show()

    //更新数据
    kuduContext.updateRows(acTransDF3,kuduTableName)
    println("更新数据后的结果：")
    df.show()

    //如果存在则更新，如果不存在，则插入
    kuduContext.upsertRows(acTransDF1,kuduTableName)
    println("更新插入数据后的结果：")
    df.show()

    //删除数据
    kuduContext.deleteRows(acTransDF2,kuduTableName)
    println("删除数据后的结果：")
    df.show()

    //删除所有数据
    kuduContext.deleteRows(acTransDF1,kuduTableName)

  }

}

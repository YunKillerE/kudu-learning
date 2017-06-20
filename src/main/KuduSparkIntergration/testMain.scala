package main.KuduSparkIntergration

import main.Utils.KuduSparkCRUD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.types.DoubleType

/**
  * Created by yunchen on 2017/6/12.
  */
object testMain {


  def main(args: Array[String]): Unit = {

    CreateTableUseSparkTest()

  }


  def CreateTableUseSparkTest(): Unit = {
    val masterList = "zjdw-pre0069:7051"
    val tablename = "spark_yunchen_supple"

    val kuduTableSchema = StructType(
      //        column name   type       nullable
      StructField("s_suppkey", IntegerType , false) ::
        StructField("s_address" , StringType, true ) ::
        StructField("s_nationkey" , IntegerType, true ) ::
        StructField("s_phone" , DoubleType, true ) ::
        StructField("s_acctbal" , StringType, true ) ::
        StructField("s_comment", StringType , true ) :: Nil)

    val kudupri = "s_suppkey"

    val kuducrud = new KuduSparkCRUD()

    kuducrud.CreateTableUseSpark(tablename,masterList,true,kuduTableSchema,kudupri,3)
  }

}

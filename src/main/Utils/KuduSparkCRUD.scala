package main.Utils

import org.apache.kudu.spark.kudu._
import org.apache.kudu.client._

import collection.JavaConverters._

/**
  * Created by yunchen on 2017/6/12.
  */
class  KuduSparkCRUD(kuduContext:KuduContext) {

  /**
    * 主要实现CRUD功能
    *
    * 1，创建表（spark建表以及调用impala建表）
    * 2，判断表是否存在
    * 3，删除表
    * 4，传入sql查询结果
    * 5，插入数据insert
    * 6，删除数据（也可以传入sql进行删除）
    *
    */

  /**
    *
    * @param kuduTableName  表名
    * @param isDelete       表存在是否删除
    * @param kuduTableSchema  表结构，可以手动创建StructType结构，也可以通过DF的schem来获取
    * @param kuduPrimaryKey   主键
    * @param kuduPartitionNum 分区数
    * @return
    */
/*  def CreateTableUseSpark(kuduTableName:String, masterList:String, isDelete:Boolean, kuduTableSchema:org.apache.spark.sql.types.StructType,
                          kuduPrimaryKey:String, kuduPartitionNum:Int)={*/

  def CreateTableUseSpark(kuduTableName:String, isDelete:Boolean, kuduTableSchema:org.apache.spark.sql.types.StructType,
                            kuduPrimaryKey:String, kuduPartitionNum:Int)={

    //1,获取kudumaster地址，创建kuduContext
    //val kuduMasters = Seq(masterList).mkString(",")
    //val kuduContext = new KuduContext(kuduMasters)

    //2，判断表是否存在，是否删除
    if (isDelete){
        tableDelete(kuduTableName)
      }
    //3，获取表格结构StructType
    val TableSchema = kuduTableSchema
    //4，获取表主键
    val PrimaryKey = Seq(kuduPrimaryKey)
    //4,创建表
    kuduContext.createTable(kuduTableName, TableSchema, Seq(kuduPrimaryKey),
      new CreateTableOptions().setNumReplicas(3).addHashPartitions(List(kuduPrimaryKey).asJava, 10))
  }

  //判断表是否存在
  def tableIsExist(kuduTableName:String): Boolean = {
    val iss = kuduContext.tableExists(kuduTableName)
    return iss

  }

  //删除表如果存在
  def tableDelete(kuduTableName:String) = {
    if(tableIsExist(kuduTableName)){
      kuduContext.deleteTable(kuduTableName)
    }
  }








}

package main.Utils

/**
  * Created by yunchen on 2017/6/12.
  */
class KuduSparkCRUD {

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
    *直接spark创建表
    * @param tableName
    */
  def CreateTableUseSpark(tableName:String, masterList:String)={

    val kuduMasters = Seq(masterList).mkString(",")



  }






}

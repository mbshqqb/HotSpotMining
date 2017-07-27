package com.tuoersi.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * Created by bigdata on 2017/7/25.
  */
object DBUtil {

  def saveToMysql(sql:String): Unit ={
    var conn: Connection = null
    var ps: PreparedStatement = null
    try {
      conn = DriverManager.getConnection("jdbc:mysql://172.17.11.161:3306/model_optimize?useUnicode=true&characterEncoding=UTF-8","root","root")
      ps = conn.prepareStatement(sql)
      ps.executeUpdate()
      println("训练完成，模型效果插入表： "+sql.split(" ")(2)+"成功！")
    } catch {
      case e: Exception => println("训练完成，模型效果插入表： "+sql.split(" ")(2)+" 失败，请检查原因！")
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }
  def saveResultToMysqlForDataFrame(sqc: SQLContext, dataFrame:DataFrame,tableName:String): Unit ={

    dataFrame.registerTempTable(tableName)
    val sqlCommand="select * from "+tableName
    val prop = new java.util.Properties
    prop.setProperty("user","root")
    prop.setProperty("password","root")
    try {
      sqc.sql(sqlCommand).write.mode(SaveMode.Append).jdbc("jdbc:mysql://172.17.11.161:3306/model_optimize?useUnicode=true&characterEncoding=UTF-8", tableName, prop)
      println("训练完成，Kmeans聚类结果写入: "+tableName+"成功！")
    }catch {
      case e: Exception => println("训练完成，Kmeans聚类结果写入: "+tableName+"失败，请检查原因！")
    }


  }


}

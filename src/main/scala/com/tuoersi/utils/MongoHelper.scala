package com.tuoersi.utils



import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.query.Imports._

import scala.collection.mutable
/**
  * Created by BigData on 2017/7/27.
  */
object MongoHelper {
  val mongodbUtil = MongoDBUtil
  var collection:MongoCollection = mongodbUtil.getCollection("test","weibo_info")


  /**
    *
    * @param where 查询的行 类型map key为查询的字段名，value为具体数值 (目前支持相等查询)
    * @param projection 要查询的列 类型map key为投影的字段名，value 1表示选择投影，0不投影
    */
  def query(where:mutable.HashMap[String,AnyRef],projection:mutable.HashMap[String,Int])={
    val cursor=collection.find(DBObject.apply(where.toList),DBObject.apply(projection.toList) )
    cursor.toList.foreach(println)


  }

  /**
    * 支持单条或者多条删除
    * @param where 对查询到的行进行删除 类型map key为查询的字段名，value为具体数值 (目前支持相等查询)
    *
    */
  def delete(where: mutable.HashMap[String,AnyRef])={
      collection.findAndRemove(DBObject.apply(where.toList))
  }

  /**
    * 目前支持单条数据插入
    * @param data 类型map key为列名，value为列的数值，即数据
    * @return
    */
  def insert(data:mutable.HashMap[String,AnyRef])={
      collection.insert(DBObject.apply(data.toList))
  }

  /**
    *
    * @param where 查询的行 类型map key为查询的字段名，value为具体数值
    * @param update 要更新行的数据 类型map key为跟新的字段名,value为具体数值
    * @param upsert True如果查询结果不存在，就直接跟新。
    * @return
    */
  def update(where: mutable.HashMap[String,AnyRef],update:mutable.HashMap[String,AnyRef],upsert:Boolean)={
    //如果不存在就直接插入
    collection.update(DBObject.apply(where.toList),DBObject.apply(update.toList),upsert = upsert)
  }

  def main(args: Array[String]): Unit = {
    val a=MongoDBObject()
    val helper =MongoHelper
    val queryMap =new mutable.HashMap[String,AnyRef]()
    queryMap.put("weibo_id","FedhJkXGF")
    queryMap.put("weibo_id1","FedhJkXGF1")
//    val projectMap =new mutable.HashMap[String,Int]()
//    projectMap.put("_id",0)
//    projectMap.put("weibo_id",1)
//    projectMap.put("weibo_content",1)
    //  helper.query(queryMap,projectMap)
    val updateMap = new mutable.HashMap[String,AnyRef]()
    updateMap.put("weibo_id1","233")
   // helper.insert(queryMap)
  //  helper.update(queryMap,updateMap,false)
  }
}

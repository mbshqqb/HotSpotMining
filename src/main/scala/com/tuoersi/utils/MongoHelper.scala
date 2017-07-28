package com.tuoersi.utils



import com.mongodb.casbah
import com.mongodb.casbah.{Imports, MongoClient, MongoCollection, MongoCursor}
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.query.Imports._
import com.tuoersi.utils.MongoDBUtil.mongoClient

import scala.collection.mutable
/**
  * Created by BigData on 2017/7/27.
  */
object MongoHelper {
  val mongodbUtil = MongoDBUtil
  var collection:MongoCollection = _
  val mongoClient: MongoClient =mongodbUtil.getClient()

  /**
    *
    * @param dbname 数据库名称
    * @param tablename 表名称
    * @return DBCollection 集合（表）
    */
  def setCollection(dbname:String,tablename:String)={
    collection=mongoClient.getDB(dbname).apply(tablename)
  }

  /**
    * 查询单条或者多条数据
    * @param where 查询的行 类型map key为查询的字段名，value为具体数值 (目前支持相等查询),如果Map对象为空，默认select *
    * @param projection 要查询的列 类型map key为投影的字段名，value 1表示选择投影，0不投影，如果Map对象为空，默认字段全选
    * @return 返回值根据实际业务,这里暂定为一个list[DbObject] DBObject 类似Map,可以直接理解为List[Map]，通过键值对得到对应值
    */
  def query(where:mutable.HashMap[String,AnyRef],projection:mutable.HashMap[String,Int]):List[casbah.Imports.DBObject] ={
    val cursor: MongoCursor =collection.find(DBObject.apply(where.toList),DBObject.apply(projection.toList) )
    cursor.toList
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
    helper.setCollection("weibo","weibo_info")
    val queryMap =new mutable.HashMap[String,AnyRef]()
    queryMap.put("weibo_id","FedhJkXGF")
    queryMap.put("weibo_id1","FedhJkXGF1")
//    val projectMap =new mutable.HashMap[String,Int]()
//    projectMap.put("_id",0)
//    projectMap.put("weibo_id",1)
//    projectMap.put("weibo_content",1)
    //  helper.query(queryMap,projectMap)
   // val updateMap = new mutable.HashMap[String,AnyRef]()
   // updateMap.put("weibo_id1","233")
   // helper.insert(queryMap)
  //  helper.update(queryMap,updateMap,false)
  val cursor: List[Imports.DBObject] =helper.query(new mutable.HashMap[String,AnyRef](),new mutable.HashMap[String,Int]())

  }
}

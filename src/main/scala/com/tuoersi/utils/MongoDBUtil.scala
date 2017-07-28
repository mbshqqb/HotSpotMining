package com.tuoersi.utils

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.{DBCollection, MongoCredential, ServerAddress}
import com.mongodb.casbah.{MongoClient, MongoCollection}

/**
  * Created by BigData on 2017/7/27.
  */
object MongoDBUtil {
  //233 ！ 
  val server = new ServerAddress("172.17.11.169", 27017)
  val credentials=MongoCredential.createScramSha1Credential("root","admin","root".toArray)
  val mongoClient: MongoClient = MongoClient(server, List(credentials))

  def getClient():MongoClient={
      mongoClient
  }

  def colseCollection()={
    if(mongoClient!=null){
        mongoClient.close()
    }
  }

}

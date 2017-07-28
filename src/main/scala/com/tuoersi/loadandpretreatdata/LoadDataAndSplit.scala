package com.tuoersi.loadandpretreatdata

import com.mongodb.casbah.{Imports, MongoCursor}
import com.tuoersi.{WeiboIdAndWords, WeiboIdAndWords2}
import com.tuoersi.utils.MongoHelper
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import utils.AnaylyzerTools

import scala.collection.mutable
import scala.collection.mutable.HashSet

/**
  * Created by bigdata on 2017/7/25.
  */
object LoadDataAndSplit {
  <!--从hdfs加载数据，传进SparkContext和hdfs路径，返回一个装填OnrRowData的dataframe-->
  /*
  因为从hdfs加载的数据没有唯一id，所以使用.zipWithUniqueId()方法，设置每条微博都有一个固定的id。但是这种方法用到两次
  map，效率过低。花费时间是直接读取有id数据的1.6倍左右。
  */
  def loadDataFromHdfsAndPretreat(sc:SparkContext,sqc:SQLContext,fileUrl:String):DataFrame = {
    println("------------------------------1.从hdfs进行读取数据并进行分词操作-------------------------------------")
    val originalRDD = sc.textFile(fileUrl).zipWithUniqueId()
    println("读取数据完毕，正在分词，请耐心等待...")
    val weiboIdAndWordsRDD = originalRDD.map({line =>(line._1.split(","),line._2.toInt)}).filter(tuple => !tuple._1(0).equals("\"nick_name\"")).filter(_._1(3).length<=4).map(p =>
    {val data = p._1
      WeiboIdAndWords(p._2,
        /*
        此处使用用java封装好的api进行分词。传进该api一个中文string，返回一个由独立单词组成的newString。
        对这个newString进行简单处理并split之后，生成一个newArray，我们称这个newArray为分词结果，它是一个盛装String，即各个独立单词的Array，和case class：OneRowData匹配。
        之所以要以Array[String]存储最后的分词结果，是因为lda模型只接受Array[String]的变量。
        */
        AnaylyzerTools.anaylyzerWords(data(2).replace("！"," ")).split(" "))}
    )
    import sqc.implicits._
    val weiboIdAndWordsDF = weiboIdAndWordsRDD.toDF.distinct()
    weiboIdAndWordsDF
  }

  def loadDataFromMongodbAndPretreat(sc:SparkContext,sqc:SQLContext): DataFrame ={//吴裕鑫负责将数据从mangoDB读入
    println("------------------------------1.从mongoDB进行读取数据并进行分词操作-------------------------------------")
    val helper = MongoHelper
    helper.setCollection("weibo","weibo_info")
    val list: List[Imports.DBObject] =helper.query(new mutable.HashMap[String,AnyRef](),new mutable.HashMap[String,Int]())

    val set=  HashSet[(String,String)]()
    list.foreach(x=>{
      val weibo_id = x.get("weibo_id").toString
      val weibo_content = x.get("weibo_content").toString
      set.add((weibo_id,weibo_content))}
    )

    //RDD[(weibo_id,weibo_content)]
    val originalRDD: RDD[(String, String)] =sc.makeRDD(set.toList)
    println("读取数据完毕，正在分词，请耐心等待...")
    val weiboIdAndWordsRDD2 = originalRDD.map(x=>{
      val content =x._2
      WeiboIdAndWords2(x._1,AnaylyzerTools.anaylyzerWords(content.replaceAll("[“”！,]","").trim).trim.split(" "))
    })
    import  sqc.implicits._
    val weiboIdAndWordsDF2=weiboIdAndWordsRDD2.toDF().distinct()
    helper.mongoClient.close()
    weiboIdAndWordsDF2
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val DF=loadDataFromMongodbAndPretreat(sc,sqlContext)
    DF.show(100)
  }

}

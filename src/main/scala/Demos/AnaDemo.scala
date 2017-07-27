package Demos

/**
  * Created by bigdata on 2017/7/23.
  */

import org.apache.spark._
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.sql.SQLContext
import utils.AnaylyzerTools

/**
  * Created by Administrator on 2016/4/6.
  */
object AnaDemo {

  //分词排序后取出词频最高的前10个
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("my app").setMaster("local[*]")
    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
//    val line1 = "【不想吓到顾客，毁容小哥烈日下头戴黑面纱送外卖】#商妹儿直播# 在成都街头有一位神秘的外卖哥，头戴斗笠盖着面纱看不见真容，真相其实是因为手二次伤残、脸深度烧伤，怕顾客吓到。虽然辛苦，但小哥很乐观：“有了这份工作，想赚了钱以后能把脸治治。” O微博直播 . '".replace("【","").replace("】","")
//    val line2 = "高温“烤”验下的坚守 主播体验地铁建设者工作日常 #高温你在哪里#  #高温下的坚守#（来自L一直播:济南时报正在直播）APP下载地址：O网页链接 2济南 ​​​​"
//    val line3 = "#昆明新闻#【天漏了！昆明全城连夜暴雨交通瘫痪】一夜暴雨，昆明北站隧道变成海洋世界，街道变河道！L秒拍视频 ​​​​"
//    val documentDF = sqlContext.createDataFrame(Seq(
//      AnaylyzerTools.anaylyzerWords(line1.replaceAll("[,]","")).toString.split(","),
//      AnaylyzerTools.anaylyzerWords(line2).toString.split(","),
//      AnaylyzerTools.anaylyzerWords(line3).toString.split(",")
//    ).map(Tuple1.apply)).toDF("text")
//
//    documentDF.show()
//    val countVec = new CountVectorizer()
//      .setInputCol("text").setOutputCol("features")
//    val countVecModel = countVec.fit(documentDF)
//    val dataFrameForLDA = countVecModel.transform(documentDF)
//    dataFrameForLDA.show(10)
val line1 = "【不想吓到顾客，毁容小哥烈日下头戴黑面纱送外卖】#商妹儿直播# 在成都街头有一位神秘的外卖哥，头戴斗笠盖着面纱看不见真容，真相其实是因为手二次伤残、脸深度烧伤，怕顾客吓到。虽然辛苦，但小哥很乐观：“有了这份工作，想赚了钱以后能把脸治治。” O微博直播 . '"
    val list = AnaylyzerTools.anaylyzerWords(line1).toString.replace("[","").replace("]","").replace(" ","").split(",")
    for(a <- list){
      println(a)
    }




  }
}
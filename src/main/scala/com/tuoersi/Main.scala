package com.tuoersi

/**
 * Hello world!
 *
 */


import com.tuoersi.loadandpretreatdata.LoadDataAndSplit
import com.tuoersi.thememining.LDAModel
import com.tuoersi.topicmining.KmeansModel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.sql.SQLContext




object Main {
  <!--更新日志：

  V1.1
  1.更新项目名称为HotSpotMining，避免和话题混淆；更新了项目架构，将整个项目各个阶段进一步解耦，但是每个人负责的部分不变；更新了saveToMysql的api；更新了分词器：识别的词语更多，避免了控制台一直输出的情形；更新停用词库，现在能够剔除“秒拍视频”、“展开全文”之类的词语；剔除了不可见字符；kmeans聚类结果能写入mysql中；加入了一些控制台输出语句，更人性化了一些；把代码中出现的hadoop0等主机名都换成了ip
  2.解决了了一个开发中遇到的问题：org.apache.spark.sql.AnalysisException: Table not found:
  原因：在一个sqlontext（或hiveContext）中registerTempTable的表不能在另一个sqlContext（或hiveContext）中使用
  所以，在loadDataFromHdfsAndPretreat中传入了sqc，保证全局唯一sqc
  3.在这个项目中没有加入的部分有：李炎的javaWeb项目部分；已经加入的部分有：李旺的贝叶斯过滤器部分、吴裕鑫的MangoDB读取数据部分、张靖的热点话题选取部分、董天航的微博情感分析部分。如有需求，可以在各自的包下添加文件，目前还想加入根据已有模型进行预测的功能。


  V1.0
  0.规定一下项目中常出现的三个术语，以免产生分歧：
  主题（theme）：一些微博聚类形成的类群的中心思想。比如100条微博被聚到一类，那这100条微博有一个共同的主题，类比kmean的核心。
  话题（topic）：一条微博可能涉及多个方面，每一个方面称为一个话题。
  话题词（term）：一个话题可以用多个话题词做总结，这些词被称为话题词。
  总结起来，一个主题由多个话题组成，一个话题包含多个话题词。
  1.整个项目使用jdk1.8编译，在跑程序之前记得更换jdk。
  2.如果要继续使用hdfs读取文件，记得将core-site.xml和hdfs-site.xml更换成自己集群的；另外，mysql的连接和库也要换成自己的，详见Utils.DBUtil.
  3.为了保持整体风格单一、美观，建议大家在更改代码的时候使用大小驼峰命名规则：类名和包名首字母全部大写，如KmeansModel；变量和方法名第一个单词小写，剩下的单词首字母大写，如loadDataFromHdfsAndPretreat。
  4.在跑程序时，会发现控制台“Word: "#" format error. -ignored”等的输出。那是分词api报的结果，不必理会；另外，该中文分词准确率官方宣称在98%以上，结果应该是可信的。

  整个项目亟待优化的地方：
  1.lda模型在有干扰词的情况下效果不佳，常出现的干扰词有:“展开全文”，“秒拍视频”，现在还没有去除的方法，希望在数据源部分直接处理掉。//已解决
  2.countVecModel模型构造词向量时，可以看到每个文本都有一个必定存在的词，致使单词向量化时，有一维的变量是相同的。最终导致整个模型效果欠佳。//已解决
  3.数据量过小，现在急需换上较大的数据。
  4.整个模型涉及五个模块：分词与构造向量、LDA模型、Kmeans模型、热点发现、情感分析。涉及参数众多，希望大家共同调试，在有限的时间内构造出最佳的模型。

  另外，对于这个项目结构不合理的地方，一定要多多指正。我遇到的其他一些问题会发在群里。
  祝整个项目圆满成功！

  -->
  def main(args:Array[String]){
    val conf = new SparkConf().setAppName("HotSpotMining").setMaster("local[*]").setExecutorEnv("file.encoding","UTF-8")
      .set("spark.driver.extraJavaOptions","-Xms1024m -Xmx1024m -XX:PermSize=1024M -XX:MaxPermSize=1024M")
    val sc = new SparkContext(conf)
    val sqc = new SQLContext(sc)

    println("-------------------------------------------程序开始运行！-------------------------------------------")
    val weiboIdAndWordsDF = LoadDataAndSplit.loadDataFromMongodbAndPretreat(sc,sqc)
    //val weiboIdAndWordsDF = LoadDataAndSplit.loadDataFromHdfsAndPretreat(sc,sqc,"hdfs://172.17.11.208:9000/data/csvFile/weibo0.csv")
    println("分词成功，返回一个DataFrame!接下来打印 该DataFrame：")
    weiboIdAndWordsDF.show(10)


    println("---------------------------------------------2.构造词向量-------------------------------------------")
    val countVec = new CountVectorizer()
      .setInputCol("words").setOutputCol("features").setMinDF(1.2)
    val countVecModel = countVec.fit(weiboIdAndWordsDF)
    val dataFrameForLDA = countVecModel.transform(weiboIdAndWordsDF)
    //dataFrameForLDA.toJavaRDD.saveAsTextFile("hdfs://172.17.11.208:9000/data/target/countvec")
    println("词向量构造成功！接下来打印 词向量DataFrame： ")
    dataFrameForLDA.show(10)
    dataFrameForLDA.cache()
    println("接下来打印 索引0-4对应的词语： ")
    println("IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII")
    println("->"+countVecModel.vocabulary(0)+"<-")
    println("->"+countVecModel.vocabulary(1)+"<-")
    println("->"+countVecModel.vocabulary(2)+"<-")
    println("->"+countVecModel.vocabulary(3)+"<-")
    println("->"+countVecModel.vocabulary(4)+"<-")
    println("IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII")


    val ldaModel = LDAModel.fitAndSaveToMysql(9,90,dataFrameForLDA)
    println("LDA训练成功，已将模型评估效果存档并返回一个LDAModel！")
    val topicsDistri = ldaModel.transform(dataFrameForLDA)
    println("接下来打印 LDA模型中的话题分布：")
    topicsDistri.show(10)
    val maxTermsPerTopic = ldaModel.describeTopics(3)
    println("接下来打印 LDA模型中每个话题最重要的三个词：")
    maxTermsPerTopic.show()



    val dataFrameForKmeans = topicsDistri.select("weibo_id",ldaModel.getTopicDistributionCol)
    val kmeansModel = KmeansModel.fitAndSaveToMysql(20,70,dataFrameForKmeans)
    println("Kmeans训练成功，已将模型评估效果存档并返回一个KmeansModel！")
    val kmeansResult = kmeansModel.transform(dataFrameForKmeans).select("weibo_id","cluster")
    utils.DBUtil.saveResultToMysqlForDataFrame(sqc,kmeansResult,"kmeans_result")
    println("接下来打印 Kmeans聚类结果：")
    kmeansResult.show(10)


    //------------------------------------5.根据已有模型预测新数据-------------------------------------------



  }
}
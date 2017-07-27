package com.tuoersi.topicmining

import cn.tuoersi.Utils.DBUtil
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.sql.DataFrame
/**
  * Created by bigdata on 2017/7/25.
  */
object KmeansModel {
  <!--传进的参数分别是，kmeans核心数量，迭代次数，一个二维的dataframe，返回一个训练过的kmeans模型-->
  def fitAndSaveToMysql( k:Int,iter:Int,twoColMatrix : DataFrame): KMeansModel ={
    val start = System.nanoTime()

    println("-----------------------------------------------4.训练Kmeans模型------------------------------------------------")
    println("当前Kmeans模型cluster数量为： "+k+"，迭代次数为： "+iter+"。")
    println("训练中，请耐心等待...")
    val kmeans=new KMeans()
      .setK(k)
      .setMaxIter(iter)
      .setFeaturesCol("topicDistribution")
      .setPredictionCol("cluster")

    val kmeansModel = kmeans.fit(twoColMatrix)



    val wssse = kmeansModel.computeCost(twoColMatrix).toFloat//均方误差
    val elapsedTime = (System.nanoTime() - start) / 1e9
    <!--将kmeans模型的评判效果（以WSSSE：均方误差为评判标准）存进mysql里-->
    val kmeansEffectSql = "insert into kmeans_wssse_topic5 (k,iter,date,wssse,elapsed_time) values ('"+k+"','"+iter+"',now(),'"+wssse+"','"+elapsedTime+"')"
    DBUtil.saveToMysql(kmeansEffectSql)
    kmeansModel
  }

}

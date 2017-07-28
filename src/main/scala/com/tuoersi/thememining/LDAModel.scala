package com.tuoersi.thememining

/**
  * Created by bigdata on 2017/7/24.
  */

import com.tuoersi.utils.DBUtil
import org.apache.spark.ml.clustering.{LDA, LDAModel}
import org.apache.spark.sql.DataFrame



object LDAModel {

    <!--传进的参数分别是，lda模型topic数量，迭代次数，一个dataframe（该dataframe必须含有feature特征列），返回一个训练过的lda模型-->
    def fitAndSaveToMysql(k:Int,iter:Int,dataframe:DataFrame): LDAModel={ //
      println("-------------------------------------------3.训练LDA模型----------------------------------------------")
      println("当前LDA模型topic数量为： "+k+"，迭代次数为： "+iter+"。")

      println("训练中，请耐心等待...")

      val start = System.nanoTime()
      val lda=new LDA()
        .setK(k)
        .setTopicConcentration(2)
        .setDocConcentration(2)
        .setOptimizer("online")
        .setCheckpointInterval(10)
        .setMaxIter(iter)

      val ldaModel=lda.fit(dataframe)



      val ll = ldaModel.logLikelihood(dataframe)
      val lp = ldaModel.logPerplexity(dataframe)
      val elapsedTime = (System.nanoTime() - start) / 1e9
      val iterator = (k,iter,ll,lp,elapsedTime)
      <!--将lda模型的训练结果（以Likelihood和Perplexity评判标准）存进mysql里-->
      val ldaEffectSql = "insert into lda_likelihood_perplexity (k,iter,date,likelihood,perplexity,elapsed_time) values ('"+k+"','"+iter+"',now(),'"+ll+"','"+lp+"','"+elapsedTime+"')"
      DBUtil.saveToMysql(ldaEffectSql)
      ldaModel
    }


}

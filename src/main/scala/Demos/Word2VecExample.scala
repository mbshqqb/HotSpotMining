package Demos

import org.apache.spark.ml.feature.Word2Vec
// $example off$
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Word2VecExample {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Word2Vec example").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // $example on$
    // Input data: Each row is a bag of words from a sentence or document.
    val documentDF = sqlContext.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    documentDF.show(10)
    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)
    val result = model.transform(documentDF)
    result.show(100)
    result.toJavaRDD.saveAsTextFile("hdfs://hadoop0:9000/data/target/word2demo_6")
    println("写入hdfs成功！！！！")

    val vecs = model.getVectors
    vecs.show(true)
    // $example off$
  }
}
package com.tuoersi

/**
  * Created by bigdata on 2017/7/25.
  */
//从左至右分别是微博id，分词结果
case class WeiboIdAndWords(weibo_id:Int, words:Array[String])

case class WeiboIdAndWords2(weibo_id:String, words:Array[String])


package com.myspark.guigu

import org.apache.spark.{SparkConf, SparkContext}

object Hello {

  def main(args: Array[String]): Unit = {
    // 1. 创建一个SparkContext  打包的时候, 把master的设置去掉, 在提交的时候使用 --maser 来设置master
    val conf: SparkConf = new SparkConf().setAppName("Hello")
    conf.setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val wordCountRdd = sc.textFile("wordCount.txt")
    val linesRdd = wordCountRdd.flatMap(_.split(" "))
    val resultRdd = linesRdd.map((_,1)).reduceByKey(_+_)
    //resultRdd.collect().foreach(println(_))
    resultRdd.foreach(println(_))//action 算子，在不同的节点中打印
  }
}

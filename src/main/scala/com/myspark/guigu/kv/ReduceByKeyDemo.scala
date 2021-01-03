package com.myspark.guigu.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReduceByKeyDemo {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ReduceByKey").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array("hello", "hello", "world", "hello", "atguigu", "hello", "atguigu", "atguigu"))
    //注意key为world的单词只有一个，所以没有走reduceByKey中的函数
    val result: RDD[(String, Int)] = rdd1.map((_,1)).reduceByKey((x, y) => 4)
    result.collect().foreach(println(_))
  }
}

package com.myspark.guigu.doublev

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Double1 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Double1").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list1 = List(30, 50, 70, 60, 10, 20, 30)  // m
    val list2 = List(30, 5, 7, 60, 1, 2, 30)  // n
    val rdd1: RDD[Int] = sc.parallelize(list1, 2)
    val rdd2: RDD[Int] = sc.parallelize(list2, 3)
    // 并集
    val rdd3: RDD[Int] = rdd1.union(rdd2)
    rdd3.collect.foreach(println(_))
  }
}

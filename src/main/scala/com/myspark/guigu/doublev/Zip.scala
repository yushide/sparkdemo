package com.myspark.guigu.doublev

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Zip {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GroupByKey").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list1 = List(30, 50, 70, 60, 10, 20, 30, 40, 60) // m
    val list2 = List(30, 5, 7, 60, 1, 2) // n
    val rdd1: RDD[Int] = sc.parallelize(list1, 2)
    val rdd2: RDD[Int] = sc.parallelize(list2, 2)
  //  val zipRdd: RDD[(Int, Int)] = rdd1.zip(rdd2)
    rdd1.zipWithIndex().collect().foreach(println(_))
    // 拉链: 1. 对应的分区的元素的个数应该一样  2. 分区数也要一样
    // 总结: 1. 总的元素个数相等 2. 分区数相等
    //zipRdd.collect().foreach(println(_))
  }
}

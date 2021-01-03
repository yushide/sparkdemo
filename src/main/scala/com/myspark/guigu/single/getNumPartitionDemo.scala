package com.myspark.guigu.single

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object getNumPartitionDemo {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Coalesce").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val list1 = List(30, 50, 70, 60, 10, 20)
    val rdd1: RDD[Int] = sc.parallelize(list1)
    println(rdd1.getNumPartitions)
  }
}

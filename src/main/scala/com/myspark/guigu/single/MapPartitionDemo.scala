package com.myspark.guigu.single

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionDemo {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Coalesce").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val list1 = List(30, 50, 70, 60, 10, 20,61)
    val rdd: RDD[Int] = sc.parallelize(list1)
    val resultRdd = rdd.mapPartitions(it => {
      it.map(_ * 2)
    })
    resultRdd.collect().foreach(println(_))
  }
}

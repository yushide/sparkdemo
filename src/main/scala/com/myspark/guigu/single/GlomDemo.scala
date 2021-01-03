package com.myspark.guigu.single

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GlomDemo {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Glom").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list1 = List(30, 50, 70, 60, 10, 20)
    //rdd的元素类型为Array[Int]
    val glomRdd: RDD[Array[Int]] = sc.parallelize(list1).glom()
    glomRdd.map(_.toList).collect().foreach(println(_))
  }
}

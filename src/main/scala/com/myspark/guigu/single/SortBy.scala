package com.myspark.guigu.single

import org.apache.spark.{SparkConf, SparkContext}

object SortBy {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SortBy").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
     //       val list1 = List(30, 50, 70, 60, 10, 20)
    val list1 = List("aa", "ccc", "bdddd", "d", "b")
    val rdd1 = sc.parallelize(list1, 2)
    //rdd1.sortBy(x => x).collect().foreach(println(_))
    rdd1.sortBy(_.length, false).collect().foreach(println(_))
  }
}

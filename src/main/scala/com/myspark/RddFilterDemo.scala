package com.myspark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RddFilterDemo {

  def main(args: Array[String]): Unit = {
    println(Math.random())
    println(Math.random())
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val rdd: RDD[Int] = sc.makeRDD(1 to 10)
    //条件为true的元素保留
    val filterRdd: RDD[Int] = rdd.filter(_ % 2 == 0)
    filterRdd.foreach(println(_))
    /*val listRdd: RDD[List[Int]] = sc.makeRDD(Array(List(1,2,3),List(4,5,6)))
    listRdd.map(list => {
      list.max
    }).foreach(println(_))*/
    spark.close()
  }
}

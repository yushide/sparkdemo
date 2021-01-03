package com.myspark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RddGroupByDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val rdd: RDD[Int] = sc.makeRDD(1 to 10)
    val groupByRdd: RDD[(Int, Iterable[Int])] = rdd.groupBy(_ % 2)
    groupByRdd.foreach(t => {
      val groupName = t._1
      val str: String = t._2.mkString(",")
      println("组的名称为："+ groupName + ",组的元素为："+ str)
    })
    spark.close()
  }
}

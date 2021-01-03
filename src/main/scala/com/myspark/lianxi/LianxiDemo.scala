package com.myspark.lianxi

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 统计每一个省份广告被点击次数的TOP3
  */
object LianxiDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val rdd: RDD[Int] = sc.makeRDD(1 to 10)
    val lianxiRdd: RDD[String] = sc.textFile("lianxi")
    val tupleRdd: RDD[((String, String),Int)] = lianxiRdd.map(str => {
      val text: Array[String] = str.split(" ")
      //((省份,广告),1) 1为出现的次数
      ((text(1), text(4)),1)
    })
    val reduceBykeyRdd: RDD[((String, String), Int)] = tupleRdd.reduceByKey(_+_)
    val mapRdd: RDD[(String, (String, Int))] = reduceBykeyRdd.map(t => {
      (t._1._1, (t._1._2, t._2)) //(省份,(广告,次数))
    })
    val grroupByKeyRdd: RDD[(String, Iterable[(String, Int)])] = mapRdd.groupByKey()
    val sortByRdd: RDD[(String, List[(String, Int)])] = grroupByKeyRdd.mapValues(it => {
      it.toList.sortBy(_._2).take(3)
    })
    sortByRdd.collect().foreach(println(_))
    spark.close()
  }
}

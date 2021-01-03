package com.myspark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RddMapDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val mapRdd: RDD[Int] = sc.makeRDD(1 to 10)
    mapRdd.map(data => data * 2).foreach(println)
    println("============================")
    /*
    优于Map,减少了发送给执行器的交互次数
    但当datas里面的数据过大时，会产生内存溢出
     */
    //datas是一个分区的数据，是scala集合
    mapRdd.mapPartitions(datas => {
      datas.map(data => data * 2) //这段代码调用scala集合的map,返回的也是scala集合！！！
    })
    println("============================")
    val tupleRdd: RDD[(String, Int)] = mapRdd.mapPartitionsWithIndex((index, datas) => {
      datas.map(data => ("分区号: "+ index, data))
    })
    tupleRdd.foreach(println)

    val flatMapRdd: RDD[String] = sc.makeRDD(List("hello spark","hello scala","hello hadoop"))
    flatMapRdd.flatMap(_.split(" ")).foreach(println(_))

    val testRdd = sc.makeRDD(Array(List(1,2),List(3,4)))
    testRdd.map(list => {
      list.size
    }).foreach(println(_))

    spark.close()
  }
}

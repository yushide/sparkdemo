package com.myspark.kv

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 对rdd进行重新分区，会有shuffle操作
 */
object PartitionByDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val pairRdd: RDD[(String, Int)] = sc.makeRDD(List(("张三",20),("李四",30),("王五",40),("赵六",50)),3)
    println(pairRdd.partitions.size)
    val glomRdd: RDD[Array[(String, Int)]] = pairRdd.glom()
    glomRdd.foreach(datas =>{
      println(datas.mkString(" "))
    })
    println("===============重新分区后====================")
    val partitionByRdd: RDD[(String, Int)] = pairRdd.partitionBy(new HashPartitioner(3))
    partitionByRdd.glom().foreach(datas =>{
      println(datas.mkString(" "))
    })
    spark.close()
  }
}

package com.myspark.kv

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ReduceBykeyDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val pairRdd: RDD[(String, Int)] = sc.makeRDD(List(("电脑",5000),("电脑",7000),("手机",5000),("冰箱",1510)),3)
    val reduceByKeyRdd: RDD[(String, Int)] = pairRdd.reduceByKey(_+_)
    reduceByKeyRdd.foreach(t => {
      println("key值为：" + t._1 + ",价格总计：" + t._2)
    })
    spark.close()
  }
}

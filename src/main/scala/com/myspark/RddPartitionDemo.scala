package com.myspark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RddPartitionDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10,11),3)
    //val rdd: RDD[Int] = sc.makeRDD(1 to 10,3)
    rdd.mapPartitionsWithIndex( (index,datas) =>{
      datas.map(data => {
        ("分区："+index,data)
      })
    }).foreach(println(_))
    rdd.glom().foreach(arr =>{
      //打印每个分区的数据
      println(arr.mkString(","))
    })
    //获取每个分区的最大值
    rdd.glom().map(arr => {
      arr.max
    })
    spark.close()
  }
}

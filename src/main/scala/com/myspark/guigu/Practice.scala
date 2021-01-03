package com.myspark.guigu

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Practice {

  def main(args: Array[String]): Unit = {
/*
1.	数据结构：时间戳，省份，城市，用户，广告，字段使用空格分割。
1516609143867 6 7 64 16
1516609143869 9 4 75 18
1516609143869 1 7 87 12
2.	需求: 统计出每一个省份广告被点击次数的 TOP3
*/

    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val lianxiRdd: RDD[String] = sc.textFile("agent.log")

    val provinceAndClickRdd = lianxiRdd.map(
      line => {
        val linesp: Array[String] = line.split(" ")
        ((linesp(1), linesp(4)),1)
      }
    )
    //得到每个省份，每个广告的点击次数
    val reduceRdd: RDD[((String, String), Int)] = provinceAndClickRdd.reduceByKey(_+_)
    val proviceRdd: RDD[(String, (String, Int))] = reduceRdd.map {
      case ((provice, ad), count) => {
        (provice, (ad, count))
      }
    }
    val result: RDD[(String, List[(String, Int)])] = proviceRdd.groupByKey().mapValues(it => {
      it.toList.sortBy(_._2).reverse.take(3)
    })
    result.collect().foreach(println(_))
}
}

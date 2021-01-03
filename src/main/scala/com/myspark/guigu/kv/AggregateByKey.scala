package com.myspark.guigu.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKey {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GroupByKey").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
    // 计算每个分区相同key最大值, 然后相加
    val rdd2: RDD[(String, Int)] = rdd.aggregateByKey(0)(_.max(_),_+_)
    // 2.  1. 分区内同时计算最大值和最小值  2. 分区间计算最大值的和与最小值的和
    val rdd3: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0,0))((x, y) => (x._1.max(y),x._2.min(y)), (x, y) => (x._1+y._1,x._2+y._2))
    rdd3.collect().foreach(println(_))
  }
}

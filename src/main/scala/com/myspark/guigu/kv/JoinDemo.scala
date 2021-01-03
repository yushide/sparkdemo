package com.myspark.guigu.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object JoinDemo {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Cogroup").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array((1, 10),(2, 20),(1, 100),(3, 30)),1)
    val rdd2 = sc.parallelize(Array((1, "a"),(2, "b"),(1, "aa"),(3, "c"), (4, "e")),1)
    val joinRdd: RDD[(Int, (Int, String))] = rdd1.join(rdd2)
    joinRdd.collect().foreach(println(_))
    val leftJoinRdd: RDD[(Int, (Int, Option[String]))] = rdd1.leftOuterJoin(rdd2)
    leftJoinRdd.collect().foreach(println(_))
    val fullJoinRdd: RDD[(Int, (Option[Int], Option[String]))] = rdd1.fullOuterJoin(rdd2)
    //fullJoinRdd.collect().foreach(println(_))
    fullJoinRdd.foreach{
      case (k,(optInt,optStr)) => {
        println(k + ",value=" +optInt.getOrElse(0) + "==========" + optStr.getOrElse(""))
      }
    }
  }
}

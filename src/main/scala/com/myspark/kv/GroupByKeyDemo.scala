package com.myspark.kv

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object GroupByKeyDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
 /*   val pairRdd: RDD[(String, Int)] = sc.makeRDD(List(("电脑",5000),("电脑",7000),("手机",5000),("冰箱",1510)),3)
    val groupByKeyRdd: RDD[(String, Iterable[Int])] = pairRdd.groupByKey()
    groupByKeyRdd.foreach(t => {
      println("key值为：" + t._1 + ",价格总计：" + t._2.sum)
    })
    groupByKeyRdd.map(it => {
      it._1
      it._2.toList.sortWith(_ > _)
    })*/

    //模拟社保数据(首先要根据spark.sql 加载数据到df中，再转到rdd，调用map方法把数据组拼为已身份证id为kay,剩下的信息为value的tuple结构
    val shebaoRdd = sc.makeRDD(List((1,("电脑",5000)),(1,("电脑",7000)),(2,("空调",7000)),(3,("手机",5000)),(4,("冰箱",1510))),3)
    val shebaoGroupRdd = shebaoRdd.groupByKey()//根据人分组
   val shebaoRes: RDD[(Int, String, String)] = shebaoGroupRdd.map(t => {
      val id = t._1
      val list: List[(String, Int)] = t._2.toList
      var pinlei: String = ""
      var price: String = ""
      //filter的逻辑写在spark.sql加载数据时，用where替换掉
     val sortedList = list.filter(t => t._1 != null).sortWith(_._2 > _._2)
  //   sortedList.foreach(println(_))
     val first = sortedList.head //获取第一条
     val last = sortedList.tail//获取最后一条
     sortedList.foreach(t => {
        //写加工逻辑
        pinlei = t._1.toString
        price = t._2.toString
      })
      (id, price, pinlei)
    })
  //  shebaoRes.collect().foreach(println(_))

    //模拟医保数据
    val yibaoRdd = sc.makeRDD(List((1,("内存",8000,"CN")),(1,("内存",5000,"CN")),(1,("cpu",7000,"US")),(3,("手机",5000,"FN")),(4,("冰箱",1510,"JP"))),3)
    val groupRdd = yibaoRdd.groupByKey()
    val yibaoRes: RDD[(Int, String, String)] = groupRdd.flatMap(t => {
      val key = t._1
      var result = new ListBuffer[Tuple3[Int, String, String]]

      val kindMap: Map[String, List[(String, Int, String)]] = t._2.toList.groupBy(_._1)
      kindMap.foreach(it => {
        val list: List[(String, Int, String)] = it._2
        var pinlei = ""
        var price = ""
        list.sortWith(_._2 > _._2).foreach(t => {
          //过滤逻辑
          pinlei = t._1
          price = t._2.toString
        })

        result.append((key, pinlei, price))
      })
      result
    })

    yibaoRes.collect().foreach(println(_))
    spark.close()
  }
}

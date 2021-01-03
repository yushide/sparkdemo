package com.myspark.guigu.single

import org.apache.spark.{SparkConf, SparkContext}

object DistincDemo {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Coalesce").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val list1 = List(30, 50, 70, 60, 10, 20, 10, 30, 50, 70)
    val rdd1 = sc.parallelize(list1)
    rdd1.distinct().collect().foreach(println(_))
    val rdd2 = sc.parallelize(List(User(10, "lisi"), User(20, "zs"), User(10, "ab")))
    rdd2.distinct().collect().foreach(println(_))
  }
}
case class User(age:Int,name:String){

  override def hashCode(): Int = this.age

  override def equals(obj: Any): Boolean = {
    obj match {
      case User(age,_) => this.age == age
      case _ => false
    }
  }
}
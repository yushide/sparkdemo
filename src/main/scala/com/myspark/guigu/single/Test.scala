package com.myspark.guigu.single

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用sortBy算子自定义排序
  */
object Test {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SortBy").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val personsRdd = sc.parallelize(new Person(10, "lisi") :: new Person(20, "zs") :: new Person(15, "ww") :: Nil)
    implicit val ord = new Ordering[Person] {
      override def compare(x:Person,y:Person): Int = {
        x.age - y.age //默认是升序
      }
    }
    //方式一
 //   personsRdd.sortBy(x => x.age).collect().foreach(println(_))
    //方式二
    personsRdd.sortBy(x => x,false).collect().foreach(println(_))
  }
}
case class Person(age:Int,name:String)

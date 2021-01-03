package main.scala.com.myspark

import org.apache.spark.{SparkConf, SparkContext}

object wordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("word count").setMaster("local[*]");
    val sc =new SparkContext(conf)
    //word.txt文件共51字节，2个分区，那么hadoop用51/2 去进行数据分割，就是第一个分区存25个字节，
    //第二个分区存26个字节，hadoop会一行一行去读取，判断这一行是否小于分区的存储，如果小于，继续读下一个行放到该分区中
    val wordContents = sc.textFile("d://word.txt",4)
    val tup = wordContents.flatMap(_.split(" ")).map((_,1))
    val result = tup.reduceByKey(_+_).collect()
    result.foreach(println(_))
    sc.stop()
  }
}

package com.myspark

import java.net.Socket

object ClientDemo {

  def main(args: Array[String]): Unit = {

    new Socket("localhost",8090)
  }
}

package com.myspark

import java.net.{ServerSocket, Socket}

object JavaSockertDemo {

  def main(args: Array[String]): Unit = {
    val ss = new ServerSocket(8090)
    val socket = ss.accept()
  }
}

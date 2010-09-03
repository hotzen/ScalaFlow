package dataflow
package tests

import dataflow._

object Channel1 {
  implicit val scheduler = new DaemonThreadPoolScheduler
  
  def main(args: Array[String]): Unit ={
    val ch = Channel.create[Int]
    
    // consumer-flow
    val f = flow {
      val v1 = ch()
      val v2 = ch()
      v1 + v2
    }

    // producer-flow
    flow {
      ch << 11
      ch << 31
    }
    
    println("result: " + f.get)
  }
}

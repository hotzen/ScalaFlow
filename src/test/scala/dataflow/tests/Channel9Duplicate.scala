package dataflow
package tests

import dataflow._
import scala.util.continuations._

object Channel9 extends Test {

  implicit val scheduler = new DaemonThreadPoolScheduler
  
  def run {
    val ch = Channel.create[Int]
    
    val (ch1,ch2) = ch.duplicate
    
    val f1 = flow {
      for (x <- ch1) println("CH1: " + x)
    }
    
    val f2 = flow {
      for (x <- ch2) println("CH2: " + x)
    }
    
    flow {
      ch << 1
      ch << 2
      ch << 3
      ch <<#
    }

    f1.await
    f2.await
     
    println("done")
  }
}

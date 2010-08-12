package dataflow
package tests

import dataflow._
import scala.util.continuations._

object Channel10 extends Test {

  implicit val scheduler = new DaemonThreadPoolScheduler
  
  def run {
    val ch = Channel.create[Int]
    
    val (chEven,chOdd) = ch.split(_ % 2 == 0)
    
    val f1 = flow {
      for (x <- chEven) println("chEven: " + x)
    }
    
    val f2 = flow {
      for (x <- chOdd) println("chOdd: " + x)
    }
    
    flow {
      ch << 1
      ch << 2
      ch << 3
      ch << 4
      ch <<#
    }

    f1.await
    f2.await
     
    println("done")
  }
}

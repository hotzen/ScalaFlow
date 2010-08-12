package dataflow
package tests

import dataflow._
import scala.util.continuations._

object Channel6 extends Test {

  implicit val scheduler = new DaemonThreadPoolScheduler
  
  def run {
    val ch = Channel.create[Int]
    
    val f = flow {
      val ch2 = ch.filter(_ > 2) // filter #1
      
      for (x <- ch2 if x % 2 == 0) { // filter #2 uses withFilter
        println(x)
      }
    }
    
    flow {
      ch << 1
      ch << 2
      ch << 3
      ch << 4
      ch << 5
      ch << 6
      ch <<#
    }

    f.await 
    println("done")
  }
}

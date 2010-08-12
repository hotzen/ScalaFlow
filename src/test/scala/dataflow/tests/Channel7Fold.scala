package dataflow
package tests

import dataflow._
import scala.util.continuations._

object Channel7 extends Test {

  implicit val scheduler = new DaemonThreadPoolScheduler
  
  def run {
    val ch = Channel.create[Int]
    
    val f = flow {
      ch.fold(0)(_ + _)
    }
    
    flow {
      ch << 1
      ch << 2
      ch << 3
      ch <<#
    }

    println("folded: " + f.get ) 
    println("done")
  }
}

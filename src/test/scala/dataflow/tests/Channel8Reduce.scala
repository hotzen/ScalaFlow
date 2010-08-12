package dataflow
package tests

import dataflow._
import scala.util.continuations._

object Channel8 extends Test {

  implicit val scheduler = new DaemonThreadPoolScheduler
  
  def run {
    val ch = Channel.create[Int]
    
    val f = flow {
      ch.reduce(_ + _)
    }
    
    flow {
      ch << 1
      ch << 2
      ch << 3
      ch <<#
    }

    println("reduced: " + f.get ) 
    println("done")
  }
}

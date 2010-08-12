package dataflow
package tests

import dataflow._
import scala.util.continuations._

object Channel12 extends Test {

  implicit val scheduler = new DaemonThreadPoolScheduler
  
  def run {
    val ch = Channel.create[Int]
        
    flow {
      ch << 1
      ch << 2
      ch << 3
      ch <<#
    }

    val fls = flow { ch.toList }
    
    println( fls.get )
    println("done")
  }
}

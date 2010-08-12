package dataflow
package tests

import dataflow._
import scala.util.continuations._

object Channel3 extends Test {

  implicit val scheduler = new DaemonThreadPoolScheduler
  
  def run {
    val ch = Channel.create[Int]
        
    // consumer
    val f = flow {
      for (x <- ch) { // ch.foreach(x => println...)
        println("consumed: " + x)
      }
      println("consuming finished, channel is terminated")
    }
    
    // producer
    flow {
      // does not work, as foreach is no CPS-aware function :(
      // for (i <- 1 to 100) { ch << i }
      
      var i = 0
      while (i < 10) {
        ch << i
        i = i + 1
      }
      
      // explicitly terminate, consumer gets stopped by TerminatedChannel-ControlException
      ch <<#
    }
            
    f.await 
    println("done")
  }
}

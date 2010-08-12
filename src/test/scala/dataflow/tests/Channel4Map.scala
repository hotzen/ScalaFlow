package dataflow
package tests

import dataflow._
import scala.util.continuations._

object Channel4 extends Test {

  implicit val scheduler = new DaemonThreadPoolScheduler
  
  def run {
    val ch = Channel.create[Int]
        
    // concurrent mapping of ch to ch2
    val ch2 = ch.map(_ * 2)
    
    // concurrent mapping of ch2 to ch3
    val ch3 = for (doubled <- ch2) yield {
      doubled.toDouble
    }
    
    // consumer
    val f = flow {
      for (d <- ch3) {
        println("consumed: " + d)
      }
      println("terminated, finished")
    }
    
    // producer
    flow {
      var i = 0
      while (i < 10) {
        ch << i
        i = i + 1
      }
      ch <<#
    }
            
    f.await 
    println("done")
  }
}

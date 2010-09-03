package dataflow
package tests

import dataflow._
import scala.util.continuations._

object Pipeline extends Test {

  implicit val scheduler = new DaemonThreadPoolScheduler
  
  def run {
    
    val p = new Pipeline[Int]
    
    flow {
      p put 1
      p put 2
      p put 3
    }
    
    val f = flow {
      val source = p
      val sink = p stage(_+1) stage(_*2) stage(_*1.1)
      
      for (x <- sink) {
        println(x)
      }
    }       
    
    f.await
    println("done")
  }
}

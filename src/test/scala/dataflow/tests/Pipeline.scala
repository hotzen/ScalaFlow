package dataflow
package tests

import dataflow._
import scala.util.continuations._

object Pipeline extends Test {

  implicit val scheduler = new DaemonThreadPoolScheduler
  
  def run {
    
    val p = new Pipeline[Int]
    
    val lastStage = flow {
      p.next(_+1).next(_*2).next(_*1.1)
    }.get
       
    
    val f = flow {
      for (x <- lastStage) {
        println(x)
      }
    }
    
    flow {
      p << 1
      p << 2
      p << 3
      p <<#
    }
    
    f.await
    println("done")
  }
}

package dataflow
package tests

import dataflow._
import scala.util.continuations._

object Channel14 extends Test {

  implicit val scheduler = new DaemonThreadPoolScheduler
  
  def run {
    val ch = Channel.create[Int]
    
//    val fs = for (fi <- 1 to 3) yield flow {
//      for (x <- ch) {
//        println("Flow #"+fi+": " + x)
//      }
//    }
    
    flow {
      for (x <- ch)
        println("1:"+x)
    }
    flow {
      for (x <- ch)
        println("2:"+x)
    }
    flow {
      for (x <- ch)
        println("3:"+x)
    }
    Thread.sleep(500)
    
//    println( ch.dumpStream )
//    
//    flow {
//      import DataFlowIterable._
//      for ( i <- (1 to 10).dataflow) {
//        ch << i
////        println(i + " added")
//      }
//    }
                        
//    fs.foreach(_.await)
    println("done")
  }
}

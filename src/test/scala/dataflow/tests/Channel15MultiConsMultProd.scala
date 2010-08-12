package dataflow
package tests

import dataflow._
import scala.util.continuations._

object Channel15 extends Test {

  implicit val scheduler = new DaemonThreadPoolScheduler
  
  import DataFlowIterable._
  
  def run {
    val ch = Channel.createBounded[Int](1)
    
    // producers
    val ps = List(
      flow {
        for (i <- (1 to 9).dataflow) {
          println("+ pre  << " + i)
          ch << i
          println("+ post << " + i)
        }
      },
      flow {
        for (i <- (-9 to -1).dataflow) {
          println("- pre  << " + i)
          ch << i
          println("- post  << " + i)
        }
      }
    )
        
//    val ps = for (pi <- 1 to 3) yield flow {
//      for (i <- (1 to 9).dataflow) {
//        val x = pi*10 + i
//        println("pre  " + pi + "# << " + x)
//        ch << x
//        println("post " + pi + "# << " + x)
//      }
//      ch<<#;
//    }
    
    // consumers
    val cs = for (ci <- 1 to 1) yield flow {
      for (x <- ch) {
        println("#"+ci+": " + x) 
      }
      println("#"+ci+": T")
    }

    ps.foreach(_.await)
    flow { ch <<# }
    
    cs.foreach(_.await)
    println("done")
  }
}

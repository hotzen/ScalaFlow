package dataflow
package tests

import dataflow._
import scala.util.continuations._

object Channel13 extends Test {

  implicit val scheduler = new DaemonThreadPoolScheduler
  
  def run {
    val ch = Channel.create[Int]
    
    flow {
      ch << 1
      ch << 2
      ch << 3
      ch <<#;
    }
        
    val f1 = flow {
      println("### waiting for channel having finished processing")
      ch.processed.sawait
      println("### channel has been processed")
    }
    
    def printState(ch: Channel[_]): Unit = {
      println("terminated: " + ch.isTerminated)
      println("processed: " + ch.isProcessed)
      println(ch.toString)
    }
    
    val f2 = flow {
      ch.foreach(x => {
        println(x)
        printState(ch)
        println("---")
      })
      println("foreach'd")
      printState(ch)
    }
    
                
    f1.await
    f2.await
    println("done")
  }
}

package dataflow
package tests

import dataflow._
import scala.util.continuations._

object Channel5 extends Test {

  implicit val scheduler = new DaemonThreadPoolScheduler
  
  def run {
    val ch1 = Channel.create[Int]
    flow {
      ch1 << 1
      ch1 << 2
      ch1 << 3
      ch1 <<#
    }
    
    val ch2 = Channel.create[Int]
    flow {
      ch2 << 11
      ch2 << 12
      ch2 << 13
      ch2 <<#
    }
    
    val ch = Channel.create[ Channel[Int] ]
    flow {
      ch << ch1
      ch << ch2
      ch <<#  
    }
    
    val f = flow {
      for (subCh <- ch; x <- subCh) { // flatMap
        println(x)
      }
    }
                
    f.await 
    println("done")
  }
}

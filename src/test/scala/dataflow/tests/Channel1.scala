package dataflow
package tests

import dataflow._

object Channel1 extends Test {

  implicit val scheduler = new DaemonThreadPoolScheduler
  
  def run {
    val ch = Channel.create[Int] // shortcut for: Channel.create[Tnt].newEager
        
    val f = flow {
      val v1 = ch()
      println("first: " + v1)
      
      val v2 = ch()
      println("second: " + v2)
    }
    
    // scheduling is non-deterministic channel = 1,2 or 2,1
    flow { ch << 1 }
    flow { ch << 2 }
        
    f.await 
    println("done")
  }
}
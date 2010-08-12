package dataflow
package tests

import dataflow._

object FlowResult extends Test {

  implicit val scheduler = new DaemonThreadPoolScheduler
  
  def run {
    val v = new Variable[Int]
    
    val r = flow { v() } // suspends until v is set, then returns v
    flow { v := 42 }
    
    println("flow-result: " + r.get) // Blocking
  }
}
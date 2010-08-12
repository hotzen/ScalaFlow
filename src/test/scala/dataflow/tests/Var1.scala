package dataflow
package tests

import dataflow._

object Var1 extends Test {

  implicit val scheduler = new DaemonThreadPoolScheduler
  
  def run {
    val v = new Variable[Int]
    
    flow { println( v() ) } // suspends until v is set
    flow { v := 42 }
    
    Thread.sleep(1000) // ensure flows are executed
  }
}
package dataflow
package tests

import dataflow._

object NestedFlowResult extends Test {

  implicit val scheduler = new DaemonThreadPoolScheduler
  
  def run {
    val a = new Variable[Int]
    val b = new Variable[Int]
    
    // XXX DIRTY CRAP, needs rework
    
    val r = flow {
      val x = a() // suspends
      
      val f = flow { // nested flow
        b := 2 * x   // setting b
      }
      
      f.sawait // need to sawait for [println "is-after" flow]-relation
      println("b has been set")
      
      // MUST NOT USE flow { }.await
      // inside a flow because await is a truly Thrad-blocking function.
      // (in contrast to <S>await which awaits by suspending)
      
      f
    }
    
    flow { a := 42 }
    
    flow { b() }.await
        
    println("flow-result: " + r.get) // Blocking
  }
}
package dataflow
package tests

import dataflow._
import scala.util.continuations._

object Channel11 extends Test {

  implicit val scheduler = new DaemonThreadPoolScheduler
  
  def run {
    val ch = new Channel[Int](1) // bounded, capacity=1
    
    val f = flow {
      
      println("pre <<1 \tT=" + Thread.currentThread.getName)
      ch << 1
      println("post <<1\tT=" + Thread.currentThread.getName+" " + ch)
      
      println("pre <<2 \tT=" + Thread.currentThread.getName)
      ch << 2 // gets suspended, then resumed by ch()
      println("post <<2\tT=" + Thread.currentThread.getName+" " + ch)
      
      println("pre <<3 \tT=" + Thread.currentThread.getName)
      ch << 3 // gets suspended, then resumed by ch()
      println("post <<3\tT=" + Thread.currentThread.getName+" " + ch)
      
      println("pre <<# \tT=" + Thread.currentThread.getName)
      ch <<#;
      println("post <<#\tT=" + Thread.currentThread.getName+" " + ch)
    }
      
    Thread.sleep(500)
    val f2 = flow {
      for (x <- ch) {
        Thread.sleep(500)
        println(x)
      }
      println("consumed " + ch)
    }
                
    f.await
    f2.await
    println("DONE")
  }
}

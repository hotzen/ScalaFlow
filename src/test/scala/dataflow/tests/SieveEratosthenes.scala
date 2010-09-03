package dataflow
package tests

import dataflow._
import DataFlowIterable._
import scala.util.continuations._

object SieveEratosthenes extends Test { //Benchmark {
  import java.util.concurrent.CountDownLatch
	
  implicit val scheduler = new ForkJoinScheduler
	
  
  def sieve(filter: Int, ch: Channel[Int], latch: CountDownLatch): Unit = flow {
    var next: Channel[Int] = null
    
    ch.foreach(i => {
      if (i % filter != 0) {
        if (next == null) {
          println(i)
          next = Channel.createLike[Int](ch)
          sieve(i, next, latch)
        } else {
          next << i
        }
      }
    })
    
    if (next == null) {
      latch.countDown
    } else {
      next <<#
    }
  }

  
	//def run(latch: CountDownLatch) = {
  def run() = {
	  val latch = new CountDownLatch(1)
    
    val in = Channel.createEager[Int]
		sieve(2, in, latch)
    
    flow {
	    for (i <- (2 to 1000*10).dataflow)
	      in << i
	    in <<#
		}
    
    latch.await
    println("foo")
  }
}

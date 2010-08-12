package dataflow
package tests

import dataflow._
import scala.util.continuations._
import java.util.concurrent.CountDownLatch
 
object ChannelFilterVsWithFilter extends Benchmark {

  implicit val scheduler = new DaemonThreadPoolScheduler
  
  def runFilter(latch: CountDownLatch) {
    val ch = Channel.create[Int]
    
    val ch2 = ch.filter(_ > 0)
    val ch3 = ch2.filter( x => x % 2 == 0)
    val ch4 = ch3.filter( x => (5*x) % 2 == 0 )
    val ch5 = ch4.filter( x => true )
        
    flow {
      var i = 0
      val max = 1000 * 100
      while (i < max) {
        ch << i
        i = i + 1
      }
      ch <<#
    }

    (flow {
      for (x <- ch5) {
        //println(x)
      }
    }).await 
    latch.countDown()
  }
  
  def runWithFilter(latch: CountDownLatch) {
    val ch = Channel.create[Int]
    
    val ch2 = ch.withFilter(_ > 0)
    val ch3 = ch2.withFilter( x => x % 2 == 0)
    val ch4 = ch3.withFilter( x => (5*x) % 2 == 0 )
    val ch5 = ch4.withFilter( x => true )
    
    flow {
      var i = 0
      val max = 1000 * 100
      while (i < max) {
        ch << i
        i = i + 1
      }
      ch <<#
    }

    (flow {
      for (x <- ch5) {
        //println(x)
      }
    }).await 
    latch.countDown()
  }
  
  def run(latch: CountDownLatch) = runFilter(latch)
}
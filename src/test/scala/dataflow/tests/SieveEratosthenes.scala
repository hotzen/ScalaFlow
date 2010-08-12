package dataflow
package tests

import dataflow._
import scala.util.continuations._

object SieveEratosthenes extends Benchmark {
  import java.util.concurrent.CountDownLatch
	
  implicit val scheduler = new ForkJoinScheduler
	  
	def run(latch: CountDownLatch) = {
	  val nums = Channel.create[Int]
		//val prims = Channel[Int]

		flow {
			var num = 2
			val max = 1000 * 10
			while (num < max) {
				nums << num				  			
				num = num + 1
			}
			nums.<<#
		}
		
		def sieve(prim: Int, ch: Channel[Int]): Unit = flow {
//	    prims << prim
	    
		  var nextCh: Option[ Channel[Int] ] = None
		  
		  ch.foreach(x => {
	      
	      // not filtered by this channel
	      if (x % prim != 0) {
	        
	        // create next sieve-filter
	        if (!nextCh.isDefined) {
//	          println("Prim: " + x)
	          nextCh = Some( Channel.createLike[Int](ch) )
			      sieve(x, nextCh.get)
	        } else {
	          // forward non-filtered elements
            nextCh.get << x
	        }
	      }
	    })
	    
      // propagate termination  
      if (nextCh.isDefined)
        nextCh.get <<#
      else {
        latch.countDown()
//        prims <<#
      }
	  }
		sieve(2, nums)
	  
//	  val fps = flow { prims.fold[ List[Int] ]( List() )( (x,z) => x :: z) }
//	  println("PRIMS: " + fps.get.reverse.mkString(", "))
	}
}

package dataflow
package tests

import dataflow._

object Channel2 extends Test {

  implicit val scheduler = new DaemonThreadPoolScheduler
  
  def run {
    val ch = Channel.create[Int]
    
    def getOr(ch: Channel[Int], terminalValue: Int): Int @dataflow = {
      try { ch() }
      catch { case TerminatedChannel => println("channel is terminated"); terminalValue }
    }
    
    val f = flow {
      val v1 = ch()
      println("first: " + v1)
      
      val v2 = ch()
      println("second: " + v2)
      
      val v3 = getOr(ch, -1)
      println("third: " + v3)
    }
    
    flow {
      ch << 1
      ch << 2
      ch <<# // Terminate Channel
    }
    
    def printGetStats(ch: Channel[_]): Unit @dataflow = {
      try {
        ch.take
        ()
      } catch { case TerminatedChannel => {
        println("channel: " + ch.toString)
        dfunit
      }}
    }
    
    Thread.sleep(1000)
    flow {
      println("ensuring the channel.size does not decrement further on .get on terminated channel")
      printGetStats(ch)
      printGetStats(ch)
      printGetStats(ch)
    }
        
    f.await 
    println("done")
  }
}

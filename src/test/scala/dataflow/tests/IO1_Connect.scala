package dataflow
package tests

import dataflow._

object IO1 extends Test {

  implicit val scheduler = new DaemonThreadPoolScheduler
  
  def run {
    implicit val dispatcher = io.Dispatcher.start
    
    val s = new io.Socket
    val heise = new java.net.InetSocketAddress("heise.de", 80)
    
    flow { s.connect(heise) }
    
    val f = flow {
      s.connected.sawait
      println("connected to heise")
    }
    
    f.await
    println("Done")
  }
}
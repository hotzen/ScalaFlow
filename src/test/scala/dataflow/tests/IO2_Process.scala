package dataflow
package tests

import dataflow._

object IO2 extends Test {

  implicit val scheduler = new DaemonThreadPoolScheduler
  
  def run {
    import dataflow.io.Socket
    import dataflow.io.http.HttpRequest
    
    implicit val dispatcher = io.Dispatcher.start
    
    val s = new Socket

    val uri = new java.net.URI("http://www.heise.de/")
    val addr = new java.net.InetSocketAddress(uri.getHost, uri.getPort)
    println("address: " + addr.getHostName + " : " + addr.getPort)
    
    // possible:
    //flow { s.process } // processing s.read & s.write - auto-synchronized on s.connected
    //flow { s.connect(addr) }
    
    // logical:
    flow {
      s.connect(addr)
      s.process
    }
    
    // send a raw http-request
    val send = flow {
      val req = (HttpRequest GET uri)
        .withConnectionClose
            
      s.write << Socket.ByteBufferToArray( req.toBytes )
    }
    
    // receive raw bytes
    val recv = flow {
      for (bytes <- s.read) {
        println("received " + bytes.length + " bytes...")
      }
      println("finished receiving")
    }
    
    flow {
      s.connected.sawait
      println("socket is connected")
    }
    
    val disc = flow {
      s.closed.sawait
      println("socket is closed")
    }
        
    disc.await
    println("Done")
  }
}
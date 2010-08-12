package dataflow
package tests

import dataflow._
import dataflow.io.Socket
import dataflow.io.http.{HttpProcessor, HttpRequest}

object IO3 extends Test {

  implicit val scheduler = new DaemonThreadPoolScheduler
  
  def run {
    implicit val dispatcher = io.Dispatcher.start
    
    val sock = new Socket
    val http = new HttpProcessor( sock )
    
    val uri = new java.net.URI("http://www.heise.de/newsticker/")
    val req = HttpRequest GET uri
    
    flow {  
      sock.connect(req.host, req.port)
      println("connected")
      sock.process
    }
           
    flow {  
      http.request( req )
      println("requested: " + req)
      // s.request( req2 )
      // s.request( req3 ) ...
    }
    
    val f = flow {
//      for (token <- s.tokens) {
//        println("TOKEN: " + token)
//      }
     
      //TODO connection does net get closed?
      
      for (resp <- http.responses) {
        println("RESPONSE:")
        println(resp.toString)
        println("BODY: " + resp.body.length + " chars")
        //s.close // TODO does not work?
      }
    }
            
    f.await
    println("Done")
  }
}
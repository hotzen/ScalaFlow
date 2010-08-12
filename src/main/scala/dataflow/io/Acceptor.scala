package dataflow
package io

import dataflow.{Signal, SuspendingAwait} // XXX WTF needed??
import java.nio.charset.Charset

import scala.util.continuations._

import java.net.InetSocketAddress
import java.nio.channels.{SocketChannel, ServerSocketChannel}
import java.nio.{ByteBuffer, CharBuffer}

object Acceptor {

}

class Acceptor(implicit val dispatcher: Dispatcher, implicit val scheduler: Scheduler) extends util.Logging {
  import Dispatcher._
  private def cpsunit: Unit @suspendable = ()

  @volatile private var _shutdown = false

	val connections = Channel.create[Socket]

  val serverSocketCh = {
    val ch = ServerSocketChannel.open
    ch.configureBlocking(false)
    ch
  }

  def bind(addr: InetSocketAddress): Unit = {
    serverSocketCh.socket.bind( addr )
    log info ("bound to " + serverSocketCh.socket.getInetAddress)
  }
  	
  def bind(host: String, port: Int): Unit =
    bind( new InetSocketAddress(host, port) )

  def accept(addr: InetSocketAddress): SuspendingAwait = {
    bind(addr)
    accept
  }
    
  def accept(): SuspendingAwait = {
  	val doneSignal = new Signal

  	dispatcher.register(serverSocketCh, None, None)
  	val wait = dispatcher.waitFor( serverSocketCh ) _
  	
  	def registerSocket(ch: SocketChannel): Unit @suspendable = {
  	  log trace ("accepted connection: " + ch)
  	  dispatcher.register(ch, None, None)
  	  val s = new Socket(ch)
  	  s.process
  	  connections << s
  	}
  	
    scheduler execute { reset {
      while (!_shutdown) {
        registerSocket( wait(Accept) )
      }
      doneSignal.invoke
    }}

  	doneSignal
  }

  def shutdown(): Unit = {
    _shutdown = true
  }
}
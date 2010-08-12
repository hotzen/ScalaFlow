package dataflow
package io

import java.io.IOException
import java.nio.channels.{Selector, SelectionKey}
import java.nio.channels.{SelectableChannel, SocketChannel, ServerSocketChannel}
import java.nio.channels.{ClosedChannelException, AsynchronousCloseException}
import java.nio.channels.{NotYetBoundException, NonReadableChannelException, NonWritableChannelException}

object Dispatcher {
  
  def createThread(d: Dispatcher): Thread = {
    val t = new Thread(new Runnable {
      def run(): Unit = d.dispatch()
    }, "NIODispatcher")
    t.setDaemon(true)
    //XXX besser etwas höhere prio?
    //t.setPriority(Thread.NORM_PRIORITY+1)
    t
  }
  
  def start: Dispatcher = {
    val d = new Dispatcher
    createThread(d).start   
    d
  }

  case object Accept  extends NIO_OP(SelectionKey.OP_ACCEPT)
  case object Connect extends NIO_OP(SelectionKey.OP_CONNECT)
  case object Read    extends NIO_OP(SelectionKey.OP_READ)
  case object Write   extends NIO_OP(SelectionKey.OP_WRITE)
}

sealed abstract class NIO_OP(op: Int) {
//  def hasInterest(ops: Int): Boolean = (ops & op) == op
  def addInterestTo(ops: Int): Int       = ops | op
  def removeInterestFrom(ops: Int): Int  = ops & ~op
}

class NIO_Handler {
  
  type OpHandler      = SelectableChannel => Unit
  type FailureHandler = Throwable         => Unit
  
  var onReadable: Option[OpHandler]  = None
  var onWritable: Option[OpHandler]  = None
  
  var onAccepted: Option[OpHandler]  = None
  var onConnected: Option[OpHandler] = None
  var onClosed: Option[OpHandler]    = None
  
  var onFailure: Option[FailureHandler] = None
  
  override def toString = {
    "onReadable:"    + onReadable.isDefined +
    ", onWritable:"  + onWritable.isDefined +
    ", onAccepted:"  + onAccepted.isDefined +
    ", onConnected:" + onConnected.isDefined +
    ", onClosed:"    + onClosed.isDefined +
    ", onFailure:"   + onFailure.isDefined
  }
}

class Dispatcher extends dataflow.util.Logging {
  import Dispatcher._
  
  private[io] val selector = Selector.open
  private val selectorGate = new Object
  
  @volatile private var _shutdown = false
  
  def dispatch(): Unit = {
    while (!_shutdown) {
      // wait for registrations
      selectorGate.synchronized {}
      
      // blocking selection of readiness
      val readyCnt = try {
        log trace ("selecting ...")
        val cnt = selector.select // throws java.io.IOException, java.nio.channels.ClosedSelectorException
        log trace ("selected " + cnt + " ready keys")
        cnt
      } catch { case e:Exception => {
        log error (e, "selector failed, shutting down")
        _shutdown = true
        -1
      }}

      if (readyCnt > 0) {
        val it = selector.selectedKeys.iterator
        while (it.hasNext) {
          val key = it.next 
          it.remove
          dispatchKey(key)
        }
      }
    }
  }
    
  private def dispatchKey(key: SelectionKey): Unit = {
    val handler = key.attachment.asInstanceOf[NIO_Handler]
    
    def removeInterest(op: NIO_OP): Unit =
      key.interestOps( op.removeInterestFrom(key.interestOps) )
    
    // ACCEPT
    if (key.isValid && key.isAcceptable) {
      try {
        val serverSocketCh = key.channel.asInstanceOf[ServerSocketChannel]
        val clientSocketCh = serverSocketCh.accept
        clientSocketCh.configureBlocking(false)
        log trace ("accepted new connection")
        
        removeInterest( Accept )
        handle(handler.onAccepted, clientSocketCh, "accepted")
      } catch { case e:Exception => {
        log error (e, "could not accept")
        close(key.channel) //XXX really close?
        handleFailure(handler.onFailure, e)
      }}
    }
    
    // CONNECT
    if (key.isValid && key.isConnectable) {
      try {
        val socketCh = key.channel.asInstanceOf[SocketChannel]
        log trace ("connection-pending: " + socketCh.isConnectionPending)
        
        val connected = if (socketCh.isConnectionPending) socketCh.finishConnect  
                        else                              socketCh.isConnected
        log trace ("now connected: " + connected)

        if (connected) {
          removeInterest( Connect )
          handle(handler.onConnected, socketCh, "connected")
        } else {
          log warn ("failed to establish connection, retrying on next connectable selection-key...")
        }
      } catch { case e:Exception => {
        log error (e, "could not connect")
        close(key.channel) //XXX really close?
        handleFailure(handler.onFailure, e)  
      }}
    }
        
    // READ
    if (key.isValid && key.isReadable) {
      try {
        val socketCh = key.channel.asInstanceOf[SocketChannel]
        removeInterest( Read )
        handle(handler.onReadable, socketCh, "readable")
      } catch { case e:Exception => {
        log error (e, "could not read")
        close(key.channel) //XXX really close?
        handleFailure(handler.onFailure, e)  
      }}
    }
    
    // WRITE
    if (key.isValid && key.isWritable) {
      try {
        val socketCh = key.channel.asInstanceOf[SocketChannel]
        removeInterest( Write )
        handle(handler.onWritable, socketCh, "writable")
      } catch { case e:Exception => {
        log error (e, "could not write")
        close(key.channel) //XXX really close?
        handleFailure(handler.onFailure, e)
      }}
    }
  }

  def register(ch: SelectableChannel, onClosed: Option[NIO_Handler#OpHandler] = None, onFailure: Option[NIO_Handler#FailureHandler] = None) = {
    val handler = new NIO_Handler
    handler.onClosed  = onClosed
    handler.onFailure = onFailure
    
    selectorGate.synchronized {
      selector.wakeup()
      ch.register(selector, 0, handler)
    }
  }
  
  def registerOpInterest(ch: SelectableChannel, op: NIO_OP, f: NIO_Handler#OpHandler): Unit = {
    assert(!ch.isBlocking, "channel must be in non-blocking mode")
    assert(ch.isRegistered, "channel must be registered")
    assert(ch.keyFor(selector).isValid, "channel-key is invalid")
    log trace ("registering " + op + "...")
    
    selectorGate.synchronized {
      log trace ("waking up selector...")
      selector.wakeup()
      
      val ops    = ch.keyFor(selector).interestOps
      val newOps = op.addInterestTo(ops)
      
      val handler    = ch.keyFor(selector).attachment.asInstanceOf[NIO_Handler]
      val newHandler = updateHandler(handler, op, f)

      val key = ch.register(selector, newOps, newHandler)
      log trace ("registered interest-ops: " + key.interestOps + ", new handler: " + key.attachment)
    }
  }

  
  def close(ch: SelectableChannel): Unit = {
    assert(ch.isRegistered, "channel must be registered with selector")
    val handler = ch.keyFor(selector).attachment.asInstanceOf[NIO_Handler]
    try {
      log trace ("closing channel...")
      ch.close()
      log trace ("channel successfully closed")
    } catch { case e:Exception => 
      log warn ("failed to gracefully close the channel: " + e.getMessage)
    } finally {
      handle(handler.onClosed, ch, "closed")
    }
  }
  
  private def handle(handler: Option[NIO_Handler#OpHandler], ch: SelectableChannel, op: String): Unit =
    handler match {
      case Some(f) => f(ch)
      case None    => log warn (op + " not handled")
    }
  
  private def handleFailure(handler: Option[NIO_Handler#FailureHandler], failure: Throwable): Unit =
    handler match {
      case Some(f) => f(failure)
      case None    => log warn ("failure not handled: " + failure.getMessage)
    }
  
  import scala.util.continuations._
  def waitFor(ch: SelectableChannel)(op: NIO_OP): SelectableChannel @suspendable =
    shift { k: NIO_Handler#OpHandler =>
      registerOpInterest(ch, op, k)
    }
     
  private def updateHandler(handler: NIO_Handler, op: NIO_OP, f: NIO_Handler#OpHandler): NIO_Handler = {
    op match {
      case Accept  => handler.onAccepted  = Some(f)
      case Connect => handler.onConnected = Some(f)
      case Read    => handler.onReadable  = Some(f)
      case Write   => handler.onWritable  = Some(f)
      case _       => log warn ("trying to update handler for invalid op: " + op)
    }
    handler
  }
  
  def shutdown = {
    _shutdown = true
    selector.wakeup
  }
}
package dataflow
package io

//import Flow._
import dataflow.{Signal, SuspendingAwait} // XXX WTF needed??
import java.nio.charset.Charset

import scala.util.continuations._

import java.net.InetSocketAddress
import java.nio.channels.{SelectionKey, SocketChannel, SelectableChannel}
import java.nio.channels.{ReadableByteChannel, WritableByteChannel}
import java.nio.{ByteBuffer, CharBuffer}


object Socket {
  val ReadBufferSize = 2048
  
  def createSocketChannel: SocketChannel = {
    val ch = SocketChannel.open
  	ch.configureBlocking(false)
  	ch
  }
  
  def ByteBufferToArray(buf: ByteBuffer): Array[Byte] = {
    val bytes = new Array[Byte](buf.remaining) 
    buf.get(bytes)
    bytes
  }
}

class Socket(val socketCh: SocketChannel = Socket.createSocketChannel)(implicit val dispatcher: Dispatcher, implicit val scheduler: Scheduler) extends util.Logging {
	import Socket._
	import Dispatcher._
	private def cpsunit: Unit @suspendable = ()
	
	private val readCh  = Channel.create[Array[Byte]]
	private val writeCh = Channel.create[Array[Byte]]
	
  def read: ChannelTake[Array[Byte]] = readCh
  def write: ChannelPut[Array[Byte]] with ChannelTerminate[Array[Byte]] = writeCh
  
	private val connectedSignal = new Signal
	def connected: SuspendingAwait = connectedSignal
	
	private val closedSignal = new Signal
	def closed: SuspendingAwait = closedSignal
	
	@volatile private var flagConnected = false 
	@volatile private var flagClosed    = false
	@volatile private var flagFailed    = false
		
	def isConnected: Boolean = flagConnected
	def isClosed: Boolean    = flagClosed
	def isFailed: Boolean    = flagFailed
	
	protected val readBuffer  = ByteBuffer.allocateDirect(ReadBufferSize)
	
	// check whether the passed socketCh is already connected (e.g. accepted)
	if (socketCh.isConnected) {
    flagConnected = true
    connectedSignal.invoke
  }
	
	
	def connect(host: String, port: Int): Unit @suspendable =
	  connect( new InetSocketAddress(host, port) )
	
  def connect(address: InetSocketAddress): Unit @suspendable = {
		assert(socketCh.isOpen, "socket not open")
		assert(!socketCh.isConnected, "socket already connected")
		assert(!socketCh.isConnectionPending, "socket is connecting")
				
		socketCh.connect(address)
		dispatcher.register(socketCh, Some(onClosed), Some(onFailure))
		
		dispatcher.waitFor(socketCh)(Connect) // suspend
		
		log info ("connection established to " + address.getHostName) //XXX getHostString not accessible?!
		flagConnected = true
		connectedSignal.invoke
	}
  
  def process(): SuspendingAwait = {
    val readSignal  = processRead()
  	val writeSignal = processWrite()
  	
  	new SuspendingAwait {
  		def sawait: Boolean @suspendable = (readSignal.sawait && writeSignal.sawait)
  	}
  }
    
  def processRead(): SuspendingAwait = {
  	val doneSignal = new Signal
  	  	
  	// returns whether to continue reading (EndOfStream not reached)
  	def doRead(ch: SocketChannel): Boolean @suspendable = {
  		readBuffer.clear
			val readCnt = ch.read( readBuffer )

			if (readCnt == -1) {
			  log trace ("processRead: end of stream")
        false
			} else {
				readBuffer.flip
				val bytes = readBuffer.remaining
				readCh << Socket.ByteBufferToArray( readBuffer )
				log trace ("processRead: read " + bytes + " bytes")
				true
			}
  	}
  	
  	val wait = dispatcher.waitFor(socketCh) _
  	scheduler execute { reset {
  	  log trace ("processRead: waiting for connected-sync")
  	  connected.sawait // suspend
  	  log trace ("processRead: processing read-channel")
  		var EndOfStream = false
  		while (!EndOfStream) {
  			EndOfStream = !doRead(wait(Read))
  		}
  		dispatcher.close(socketCh)
			doneSignal.invoke
  	}}
  	
  	doneSignal
  }

  def processWrite(): SuspendingAwait = {
  	val doneSignal = new Signal

  	val wait = dispatcher.waitFor(socketCh) _
  	
  	def doWrite(buf: ByteBuffer): Unit @suspendable = {
  	  log trace ("processWrite: " + buf.remaining + " bytes to write")
  	  
  	  var byteCnt: Int = -1 // workaround for no-symbol error
  		while (buf.remaining > 0) {
  			val ch = wait( Write ) //.asInstanceOf[SocketChannel] => No-Symbol found 
  			byteCnt = ch.asInstanceOf[SocketChannel].write( buf )
  			log trace ("processWrite: wrote " + byteCnt + " bytes")
  		}
  	  log trace ("processWrite: all bytes written")
  	}
  	  	
  	scheduler execute { reset {
  	  log trace ("processWrite: waiting for connected-sync")
  	  connected.sawait // suspend
  	  log trace ("processWrite: processing write-channel")
  	  writeCh.foreach(bytes => doWrite(ByteBuffer.wrap(bytes)) )
      doneSignal.invoke
  	}}
  	
  	doneSignal
  }
  
  def close: Unit = socketCh.close
  
  private[io] def onClosed(ch: SelectableChannel): Unit = {
  	log trace ("onClosed: terminating read and write channels")
  	
  	if (!flagClosed) {
  	  flagClosed = true
  	  readCh.tryTerminate
  	  writeCh.tryTerminate
  	  closedSignal.invoke
  	}
  }
  
  private[io] def onFailure(failure: Throwable): Unit = {
    log error (failure, "onFailure: " + failure.getMessage + " - terminating read and write channels")

    if (!flagFailed) {
      flagFailed = true
      
      if (!flagClosed) {
        flagClosed = true
        readCh.terminate
        writeCh.terminate
        closedSignal.invoke
      }
    }
  }
  
  override def toString =
    "<Socket "+socketCh+" read="+readCh+" write="+writeCh+" >"
}
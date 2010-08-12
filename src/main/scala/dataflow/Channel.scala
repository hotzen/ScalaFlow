package dataflow

import scala.util.continuations._
import util.Logging

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.atomic.AtomicInteger

object TerminatedChannel extends TerminatedChannel
private[dataflow] class TerminatedChannel
  extends RuntimeException
  with scala.util.control.ControlThrowable

//TODO proper use
object BreakLoop extends BreakLoop
private[dataflow] class BreakLoop
  extends RuntimeException
  with scala.util.control.ControlThrowable


object Channel {
  
  val DefaultBound = 1000
  
  def createBounded[T](bound: Int)(implicit scheduler: Scheduler): Channel[T] =
    new Channel[T]( bound )(scheduler)
  
  def createLazy[T](implicit scheduler: Scheduler): Channel[T] =
    new Channel[T]( 1 )(scheduler) //TODO: 0?

  def createEager[T](implicit scheduler: Scheduler): Channel[T] =
    new Channel[T]( Int.MaxValue )(scheduler)
  
  def create[T](implicit scheduler: Scheduler): Channel[T] =
    createBounded[T](DefaultBound)(scheduler) // XXX prevents StackOverflow?
    //newEager[T](scheduler)
 
  def createLike[T](ch: Channel[_]): Channel[T] =
    new Channel[T]( ch.capacity )( ch.scheduler )
}

trait ChannelGet[T] { self: Channel[T] =>
  def get: T @suspendable
  def apply(): T @suspendable = get
  
  def foreach[U](f: T => U @suspendable): Unit @suspendable
  def map[M](f: T => M @suspendable): Channel[M]
  def collect[M](f: T => Option[M] @suspendable): Channel[M]
  def flatMap[M](f: T => Channel[M] @suspendable): Channel[M]
  def flatMapIter[M](f: T => Iterable[M]): Channel[M]
  def filter(p: T => Boolean @suspendable): Channel[T]
  
  //TODO withFilter? requires ChannelFilter-definition here
  
  def fold[B](z: B)(f: (T,B) => B @suspendable): B @suspendable
  def reduce[B >: T](f: (T,B) => B @suspendable): B @suspendable
  
  def duplicate(): (Channel[T], Channel[T])
  def split(p: T => Boolean @suspendable): (Channel[T], Channel[T])
  def combine[A <: T](ch2: ChannelGet[A]): Channel[T]
  def concat[A <: T](ch2: ChannelGet[A]): Channel[T]
  def transfer[B](to: ChannelPut[B], f: T => B): Unit
  def transferTerminate[B](to: ChannelPut[B] with ChannelTerminate[B], f: T => B): Unit
}

trait ChannelPut[T] { self: Channel[T] =>
  def put(value: T): Unit @suspendable
  def <<(v: T): Unit @suspendable = put(v)
  
  def putAll(values: Iterable[T]): Unit @suspendable
  def <<<(vs: Iterable[T]): Unit @suspendable = putAll(vs)
}

trait ChannelTerminate[T] { self: Channel[T] =>
  def isTerminated: Boolean
  def terminated: SuspendingAwait
  
  def isProcessed: Boolean
  def processed: SuspendingAwait
  
  def terminate: Unit
  def <<# : Unit = terminate
  
  def tryTerminate: Unit = if (!isTerminated) terminate
  def <<#? : Unit = tryTerminate
}
  
final class Channel[T](val capacity: Int)(implicit val scheduler: Scheduler) extends ChannelGet[T] with ChannelPut[T] with ChannelTerminate[T] {
  assert( capacity > 0 )
    
//  private[this] class Stream[T] {
//    val value = new Variable[T]
//    var next: Stream[T] = null
//  }
  
  private[this] class Stream[T] {
    //@volatile var value: T = null.asInstanceOf[T]
    val value = new Variable[T]
    var next: Stream[T] = null
    var last: Boolean   = false
  }
  
  type Consumer = (T => Unit)
  type Producer = (Unit => Unit)
  
  def cpsunit: Unit @suspendable = ()
  val self = this
  
  private[this] var head = new Stream[T]
  private[this] var tail = head
    
  private[this] val headLock = new ReentrantLock
  private[this] val tailLock = new ReentrantLock
  
  private[this] val suspendedProducers = new ConcurrentLinkedQueue[Producer]
  
  private[this] val counter = new AtomicInteger(0)
  
  private[this] val terminatedSignal = new Signal
  private[this] val processedSignal  = new Signal
  
  @volatile private[this] var terminatedFlag = false
  @volatile private[this] var processedFlag  = false
  
  def size: Int        = counter.get
  def isEmpty: Boolean = (size == 0)
  def isFull: Boolean  = (size >= capacity)
    
  def isLazy: Boolean  = (capacity == 1)
  def isEager: Boolean = (capacity == Int.MaxValue)
  def isBound: Boolean = !isLazy && !isEager
  

  
  def get(): T @suspendable = {
    headLock.lock
    
    // flagged as terminated and no next element
    if (terminatedFlag && head.next == null) {
      headLock.unlock
      
      if (!processedFlag) {
        processedFlag = true
        processedSignal.invoke
      }
      
      throw TerminatedChannel
    }
    
    // empty, create new empty variable for this consumer
    if (head.next == null) {
      println("get() E!")
      tailLock.lock
      head.next = new Stream[T]
      tailLock.unlock
    }
    
    val dfvar = head.value
    head = head.next
    counter.decrementAndGet
    headLock.unlock
    
    resumeProducer()
    
    dfvar.get // suspend
  }
  
  def put(v: T): Unit @suspendable = {
    if (counter.get == capacity)
      suspendProducer()
    else cpsunit
    
    tailLock.lock
    val dfvar = tail.value
    
    if (tail.next == null)
      tail.next = new Stream[T]
    
    tail = tail.next
    counter.incrementAndGet
    tailLock.unlock
    
    dfvar set v
  }
  
  private def suspendProducer(): Unit @suspendable =
    shift { k: Producer =>
      println("suspendProducer")
      suspendedProducers offer k
      
      if (counter.get < capacity && suspendedProducers.remove(k)) {
        println("suspend RACE !!!!")
      }
      
      ()
    }
  
  // resuming only one producer in FIFO-order ensures fairness
  private def resumeProducer(): Unit = {
    println("resumeProducer")
    val k = suspendedProducers.poll
    if (k != null)
      scheduler execute { k() }
  }
  
    
  def putAll(values: Iterable[T]): Unit @suspendable = {
    val it = values.iterator
    while (it.hasNext)
      this put it.next
  }  
  
  
  // TRAIT ChannelTerminate
  
  def isTerminated: Boolean = terminatedFlag
  def isProcessed: Boolean  = processedFlag
  
  def terminated: SuspendingAwait = terminatedSignal
  def processed:  SuspendingAwait = processedSignal
    
  def terminate: Unit = {
    terminatedFlag = true
    terminatedSignal.invoke
  }
  
//    if (terminatedFlag)
//      throw new DataFlowException("Channel is already terminated")
//    
//    val v = null.asInstanceOf[T]
//    
//    lastLock.lock
//    val n = new Stream[T]
//    n.value = T
//    last.next = n
//    last = n
//    lastLock.unlock
//    
//    terminatedFlag = true
//    terminatedSignal.invoke
//  }
//    
  
  
  
    
  /**
   * foreach
   * 
   * this implementation applies passed function to every element of this channel.
   * the method is not detached from the current thread,
   * so the caller is blocked until foreach has finished.
   * 
   * if the channel never gets terminated, foreach never returns.
   * 
   * THIS CHANNEL MUST NOT BE READ FROM AFTER USING THIS METHOD 
   * 
   * @return new channel
   */
  def foreach[U](f: T => U @suspendable): Unit @suspendable = {
  	val ch = this
   	try while(true)
   	  f( ch() )
  	catch { case TerminatedChannel => 
  	        case BreakLoop         =>
  	        case e:Exception       => handleEx(e) } 
  }
  
  
  /**
   * map
   * 
   * this implementation immediately returns a new channel
   * and performs the calculations concurrently.
   * 
   * THIS CHANNEL MUST NOT BE READ FROM AFTER USING THIS METHOD 
   * 
   * @return new channel
   */
  def map[M](f: T => M @suspendable): Channel[M] = {
  	val inCh  = this
  	val outCh = Channel.createLike[M](inCh)

  	scheduler execute { reset {
  	  try while(true)
  	    outCh put f( inCh() )
  		catch { case TerminatedChannel => outCh.terminate
              case BreakLoop         =>
              case e:Exception       => handleEx(e) }
  	}}
  	outCh
  }
  
  
  
  
  /**
   * collect
   * 
   * a combination of map and filter or "conditional map".
   * the given function f returns an Option:
   *   if Some(value), value is added to the newly created channel, returned by collect
   * 
   * THIS CHANNEL MUST NOT BE READ FROM AFTER USING THIS METHOD 
   * 
   * @return new channel
   */
  def collect[M](f: T => Option[M] @suspendable): Channel[M] = {
    val inCh  = this
    val outCh = Channel.createLike[M](inCh)

    scheduler execute { reset {
      try while(true) {
        val m = f( inCh() )
        if (m.isDefined) outCh put m.get
        else             cpsunit    
      }
      catch { case TerminatedChannel => outCh terminate
              case BreakLoop         =>
              case e:Exception       => handleEx(e) }
    }}
    outCh
  }
  
  
  
  /**
   * flatMap
   * 
   * this implementation immediately returns a new channel
   * and performs the calculations concurrently.
   * 
   * THIS CHANNEL MUST NOT BE READ FROM AFTER USING THIS METHOD 
   * 
   * @return new channel
   */
  def flatMap[M](f: T => Channel[M] @suspendable): Channel[M] = {
    val inCh  = this
    val outCh = Channel.createLike[M](inCh)
    
    scheduler execute reset {
      def transfer(from: Channel[M], to: Channel[M]): Unit @suspendable = {
        try while (true)
          to put from()
        catch { case TerminatedChannel => }
      }
      
      try while(true)
        transfer(f( inCh() ), outCh)
      catch { case TerminatedChannel => outCh.terminate
              case BreakLoop         =>
              case e:Exception       => handleEx(e) }
    }
    outCh
  }
  
  /**
   * flatMap-Implementation for functions returning Iterable-Implementations like Lists
   * 
   * this implementation immediately returns a new channel
   * and performs the calculations concurrently.
   * 
   * THIS CHANNEL MUST NOT BE READ FROM AFTER USING THIS METHOD 
   * 
   * @return new channel
   */
  def flatMapIter[M](f: T => Iterable[M]): Channel[M] = {
  	val inCh  = this
    val outCh = Channel.createLike[M](inCh)
    
    scheduler execute reset {
      try while(true)
        outCh putAll f( inCh() )
      catch { case TerminatedChannel => outCh.terminate
              case BreakLoop         =>
              case e:Exception       => handleEx(e) }
    }
    outCh
  }
      
  
  
  /**
   * creates a new channel containing only elements from this channel
   * which satisfy given predicate p
   * 
   * this implementation immediately returns a new channel
   * and performs the calculations concurrently.
   * 
   * THIS CHANNEL MUST NOT BE READ FROM AFTER USING THIS METHOD 
   * 
   * @return new channel
   */
  def filter(p: T => Boolean @suspendable): Channel[T] = {
    val inCh  = this
    val outCh = Channel.createLike[T](inCh)
    
    scheduler execute { reset {
      try while (true) {
        val in = inCh()
        if (p(in)) outCh put in 
        else       cpsunit
      } catch { case TerminatedChannel => outCh.terminate
                case BreakLoop         =>
                case e:Exception       => handleEx(e) }
    }}
    outCh
  }
  
  @deprecated("NOT YET TESTED")
  def withFilter(p: T => Boolean @suspendable): ChannelFilter =
    new ChannelFilter(p)
  
  
  class ChannelFilter(p: T => Boolean @suspendable) {
    
    def foreach[U](f: T => U @suspendable): Unit @suspendable = {
      val ch = self
      try while(true) {
        val v = ch()
        if (p(v)) f(v)
        else      cpsunit //XXX U vs. Unit
      } catch { case TerminatedChannel => 
                case BreakLoop         =>
                case e:Exception       => handleEx(e) }
    }
    
    def map[M](f: T => M @suspendable)(implicit scheduler: Scheduler): Channel[M] = {
      val inCh  = self
      val outCh = Channel.createLike[M](inCh)
  
      scheduler execute { reset {
        try while(true) {
          val v = inCh()
          if (p(v)) outCh put f(v)
          else      cpsunit
        } catch { case TerminatedChannel => outCh.terminate
                  case BreakLoop         =>
                  case e:Exception       => handleEx(e) }
      }}
      outCh
    }
    
    def flatMap[M](f: T => Channel[M] @suspendable): Channel[M] = {
      val inCh  = self
      val outCh = Channel.createLike[M](inCh)
      
      scheduler execute reset {
        def transfer(from: Channel[M], to: Channel[M]): Unit @suspendable = {
          try while (true)
            to put from()
          catch { case TerminatedChannel => }
        }
        
        try while(true) {
          val v = inCh()
          if (p(v)) transfer(f(v), outCh)
          else      cpsunit
        } catch { case TerminatedChannel => outCh.terminate
                  case BreakLoop         =>
                  case e:Exception       => handleEx(e) }
      }
      outCh
    }
    
    def withFilter(q: T => Boolean): ChannelFilter = 
      new ChannelFilter(x => p(x) && q(x))
  }
  
  
  
  /**
   * applies given binary function f with initial accumulator value z on this channel
   
   * THIS CHANNEL MUST NOT BE READ FROM AFTER USING THIS METHOD 
   * 
   * @return result of reduction 
   */
  def fold[B](z: B)(f: (T,B) => B @suspendable): B @suspendable = {
    var res = z  
    this.foreach(x => {
      res = f(x, res)
      cpsunit
    })
    res
  }
  
  @throws(classOf[TerminatedChannel])
  def reduce[B >: T](f: (T,B) => B @suspendable): B @suspendable = {
    val z: B = this.get
    this.fold(z)(f)
  }
  
  
  
  
  /**
   * duplicates this channel into 2 new channels,
   * both containing the same date coming from this channel.
   * 
   * this implementation immediately returns a new channel
   * and performs the calculations concurrently.
   * 
   * THIS CHANNEL MUST NOT BE READ FROM AFTER USING THIS METHOD 
   * 
   * @return Tuple of 2 new Channels
   */
  def duplicate(): (Channel[T], Channel[T]) = {
  	val inCh  = this
  	val outCh1 = Channel.createLike[T](this)
  	val outCh2 = Channel.createLike[T](this)
  	
  	scheduler execute { reset {
  	  try while(true) {
  	  	val in = inCh()
  	  	outCh1 put in
  	  	outCh2 put in
  	  } catch { case TerminatedChannel => { outCh1.terminate; outCh2.terminate; } } //XXX BreakLoop?
  	}}
  	(outCh1, outCh2)
  }

  /**
   * splits this channel into 2 new channels.
   * data satisfying predicate p goes into channel #1,
   * data not satisfying goes into channel #2
   * 
   * this implementation immediately returns a new channel
   * and performs the calculations concurrently.
   * 
   * THIS CHANNEL MUST NOT BE READ FROM AFTER USING THIS METHOD 
   * 
   * @return Tuple of 2 new Channels
   */
  def split(p: T => Boolean @suspendable): (Channel[T], Channel[T]) = {
  	val inCh  = this
  	val outCh1 = Channel.createLike[T](this)
  	val outCh2 = Channel.createLike[T](this)
  	
  	scheduler execute { reset {
  	  try while(true) {
  	  	val in = inCh()
  	  	if (p(in)) outCh1 put in
  	  	else       outCh2 put in
  	  } catch { case TerminatedChannel => { outCh1.terminate; outCh2.terminate; } //XXX BreakLoop?
  	            case e:Exception       => handleEx(e) }
  	}}
  	(outCh1, outCh2)
  }
  
  /**
   * transfers values from this channel and values of ch2 into a new channel.
   * both transfer-operations of this and ch2 into the new channel happen concurrently without any guarantee of order.
   * 
   * THIS CHANNEL MUST NOT BE READ FROM AFTER USING THIS METHOD
   * 
   * @param ch2 second channel to take values from and combine with this channel 
   * @return new channel with values of both this and second channel
   */
  @deprecated("NOT YET TESTED")
  def combine[A <: T](ch2: ChannelGet[A]): Channel[T] = {
    val ch  = Channel.createLike[T](this)
    val ch1 = this
    val ch1Done = new Signal
    val ch2Done = new Signal
    scheduler execute { reset {
      ch1.foreach(x => ch put x)
      ch1Done.invoke
    }}
    scheduler execute { reset {
      ch2.foreach(x => ch put x)
      ch2Done.invoke
    }}
    scheduler execute { reset {
      ch1Done.sawait
      ch2Done.sawait
      ch.terminate
    }}
    ch
  }
  
  /**
   * transfers values from this channel and-then values of ch2 into a new channel.
   * the transfer-operations of this and ch2 happen one after the other, so values are ordered
   * with values from this first channel, then values from the second channel.
   * this first channel has to get terminated before the transfer-operation from the second channel can start.
   * 
   * THIS CHANNEL MUST NOT BE READ FROM AFTER USING THIS METHOD
   * 
   * @param ch2
   * @return
   */
  @deprecated("NOT YET TESTED")
  def concat[A <: T](ch2: ChannelGet[A]): Channel[T] = {
    val ch  = Channel.createLike[T](this)
    val ch1 = this
    scheduler execute { reset {
      ch1.foreach(x => ch put x)
      ch2.foreach(x => ch put x)
      ch.terminate
    }}
    ch
  }

  /**
   * transfers all values from this channel, transformed by given function, into the given channel.
   * 
   * THIS CHANNEL MUST NOT BE READ FROM AFTER USING THIS METHOD
   * 
   * @param to Channel to transfer values into
   */
  @deprecated("NOT YET TESTED")
  def transfer[B](to: ChannelPut[B], f: T => B): Unit = {
    val ch = this
    scheduler execute { reset {
      ch.foreach(x => to put f(x))
    }}
  }
  
  
  /**
   * transfers all values from this channel, transformed by given function, into the given channel.
   * after transfer has been completed, the to-channel gets terminated. 
   * 
   * THIS CHANNEL MUST NOT BE READ FROM AFTER USING THIS METHOD
   * 
   * @param to Channel to transfer values into
   */
  @deprecated("NOT YET TESTED")
  def transferTerminate[B](to: ChannelPut[B] with ChannelTerminate[B], f: T => B): Unit = {
    val ch = this
    scheduler execute { reset {
      ch.foreach(x => to put f(x))
      to.terminate
    }}
  }
 
  
  def handleEx(e: Exception): Unit @suspendable = {
    //TODO handle properly
    synchronized {
      println("[" + currentThread.getName + "] Exception during Channel-Operation: " + e.toString)
      e.printStackTrace()
    }
    throw e
  }
  
  def toList(): List[T] @suspendable = {
    var xs = List[T]()
    this.foreach(xs ::= _)
    xs.reverse
  }
  
  override def toString =
    "<Channel" +
    " empty=" + isEmpty +
    "; full=" + isFull +
    "; terminated=" + isTerminated +
    "; processed="  + isProcessed +
    "; size="     + size +
    "; capacity=" + capacity +
    "; lazy="  + isLazy +
    "; eager=" + isEager +
    "; bound=" + isBound +
    ">"
}
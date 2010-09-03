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
  
  def createBounded[A](bound: Int)(implicit scheduler: Scheduler): Channel[A] =
    new Channel[A]( bound )(scheduler)
  
  def createLazy[A](implicit scheduler: Scheduler): Channel[A] =
    new Channel[A]( 1 )(scheduler) //TODO: 0?

  def createEager[A](implicit scheduler: Scheduler): Channel[A] =
    new Channel[A]( Int.MaxValue )(scheduler)
  
  def create[A](implicit scheduler: Scheduler): Channel[A] =
    createBounded[A](DefaultBound)(scheduler) // XXX prevents StackOverflow?
    //newEager[A](scheduler)
 
  def createLike[A](ch: Channel[_]): Channel[A] =
    new Channel[A]( ch.capacity )( ch.scheduler )
}

trait ChannelTake[A] { self: Channel[A] =>
  def take: A @suspendable
  def apply(): A @suspendable = take
  
  def foreach[U](f: A => U @suspendable): Unit @suspendable
  def map[M](f: A => M @suspendable): Channel[M]
  def collect[M](f: A => Option[M] @suspendable): Channel[M]
  def flatMap[M](f: A => Channel[M] @suspendable): Channel[M]
  def flatMapIter[M](f: A => Iterable[M]): Channel[M]
  def filter(p: A => Boolean @suspendable): Channel[A]
  
  //TODO withFilter? requires ChannelFilter-definition here
  
  def fold[B](z: B)(f: (A,B) => B @suspendable): B @suspendable
  def reduce[B >: A](f: (A,B) => B @suspendable): B @suspendable
  
  def duplicate(): (Channel[A], Channel[A])
  def split(p: A => Boolean @suspendable): (Channel[A], Channel[A])
  def combine[B <: A](ch2: ChannelTake[B]): Channel[A]
  def concat[B <: A](ch2: ChannelTake[B]): Channel[A]
  def transfer[B](to: ChannelPut[B], f: A => B): Unit
  def transferTerminate[B](to: ChannelPut[B] with ChannelTerminate[B], f: A => B): Unit
}

trait ChannelPut[A] { self: Channel[A] =>
  def put(value: A): Unit @suspendable
  def <<(v: A) : Unit @suspendable = put(v)
  
  def putAll(values: Iterable[A]): Unit @suspendable
  def <<<(vs: Iterable[A]) : Unit @suspendable = putAll(vs)
}

trait ChannelTerminate[A] { self: Channel[A] =>
  def isTerminated: Boolean
  def terminated: SuspendingAwait
  
  def isProcessed: Boolean
  def processed: SuspendingAwait
  
  def terminate: Unit
  def <<# : Unit = terminate
  
  def tryTerminate: Unit = if (!isTerminated) terminate
  def <<#? : Unit = tryTerminate
}
  
final class Channel[A](val capacity: Int)(implicit val scheduler: Scheduler)
  extends ChannelTake[A] with ChannelPut[A] with ChannelTerminate[A] {
  assert( capacity >= 0 )
    
  def cpsunit: Unit @suspendable = ()
  val self = this
  
  private[this] class Stream[A] {
    val value = new Variable[A]
    var next: Stream[A] = null
  }
  
  type Producer = (Unit => Unit)

  private[this] var streamTake = new Stream[A]
  private[this] var streamPut  = streamTake
      
  private[this] val lockTake = new ReentrantLock
  private[this] val lockPut  = new ReentrantLock
  
  private[this] val suspendedProducers = new ConcurrentLinkedQueue[Producer]
    
  private[this] val counter = new AtomicInteger(0)
  
  private[this] val terminatedSignal = new Signal
  private[this] val processedSignal  = new Signal
  
  @volatile private[this] var terminatedFlag = false
  @volatile private[this] var processedFlag  = false
  
  def size: Int        = counter.get
  def isEmpty: Boolean = (size <= 0)
  def isFull: Boolean  = (size >= capacity)
    
  def isLazy: Boolean  = (capacity == 1)
  def isEager: Boolean = (capacity == Int.MaxValue)
  def isBound: Boolean = !isLazy && !isEager
  
  private[this] val T = null.asInstanceOf[A] 
    
  dataflow.util.Logger.DefaultLevel = dataflow.util.LogLevel.All
  val logger = dataflow.util.Logger.get("Ch")
    
  
  def take(): A @suspendable = {
    if (terminatedFlag && streamTake.next == null) {
//      logger trace ("fast throw")
      throw TerminatedChannel
    }
    
    lockTake.lock
    
    // create new empty variable for the next consumer
    if (!terminatedFlag && (streamTake.next eq null)) {
      lockPut.lock
      if (!terminatedFlag && (streamTake.next eq null)) {
        streamTake.next = new Stream[A]
      }
      lockPut.unlock
    }
    
    val dfvar = streamTake.value
    
    // may be null on terminated channel, then just stay here
    if (streamTake.next ne null) {
      streamTake = streamTake.next
      counter.decrementAndGet // may be < 0
    }
        
    lockTake.unlock
    
    resumeProducer()
    
    val v = dfvar.get // suspend
    if (v == T)
      throw TerminatedChannel
    v
  }
  
  
  def put(v: A): Unit @suspendable = {
    if (terminatedFlag)
      throw new Exception("Channel is terminated")
    
    lockPut.lock
    counter.incrementAndGet

    if (isFull) // can only decrease
      suspendProducer(lockPut)
    else
      cpsunit
    
    lockPut.lock

    //counter.incrementAndGet
    val dfvar = streamPut.value
    if (streamPut.next eq null) {
      streamPut.next = new Stream[A]
    }
    streamPut = streamPut.next
    
    while (lockPut.getHoldCount > 0)
      lockPut.unlock

    dfvar set v
  }
      
  private def suspendProducer(lock: ReentrantLock): Unit @suspendable = {
    shift { k: Producer =>
      if (!isFull) { // Race #1
        println("ch suspend race#1")
        k()
      } else {
        assert( suspendedProducers offer k )
        if (!isFull && suspendedProducers.remove(k)) { // Race #2
          println("ch suspend race#2")
          k()
        } else {
          println("ch suspend unlock")
          lock.unlock
        }
      }
    }
  }
      
  
  // resuming only one producer in FIFO-order ensures fairness
  private def resumeProducer(): Unit = {
    val k = suspendedProducers.poll
    if (k != null) {
      //logger trace ("resuming... counter=" + counter.get)
      scheduler execute { k() }
    } else {
      //logger trace ("nothing to resume")
    }
  }
  
    
  def putAll(values: Iterable[A]): Unit @suspendable = {
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
    if (terminatedFlag)
      throw new DataFlowException("Channel already terminated")
    
    terminatedFlag = true
    
    lockTake.lock
    lockPut.lock
    
    streamPut.value := T

    while (streamPut.next != null) {
      streamPut = streamPut.next
      streamPut.value := T
    }
    
    lockPut.unlock
    lockTake.unlock
    
    terminatedSignal.invoke
  }
  
//    if (terminatedFlag)
//      throw new DataFlowException("Channel is already terminated")
//    
//    val v = null.asInstanceOf[A]
//    
//    lastLock.lock
//    val n = new Stream[A]
//    n.value = A
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
  def foreach[U](f: A => U @suspendable): Unit @suspendable = {
  	val ch = this
  	try loop {
  	  f( ch.take )
  	}	catch { case TerminatedChannel => 
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
  def map[M](f: A => M @suspendable): Channel[M] = {
  	val inCh  = this
  	val outCh = Channel.createLike[M](inCh)

  	scheduler execute { reset {
  	  try loop {
  	    outCh put f( inCh.take )
  	  } catch { case TerminatedChannel => outCh.terminate
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
  def collect[M](f: A => Option[M] @suspendable): Channel[M] = {
    val inCh  = this
    val outCh = Channel.createLike[M](inCh)

    scheduler execute { reset {
      try loop {
        val m = f( inCh.take )
        if (m.isDefined) outCh put m.get
        else             cpsunit    
      } catch { case TerminatedChannel => outCh terminate
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
  def flatMap[M](f: A => Channel[M] @suspendable): Channel[M] = {
    val inCh  = this
    val outCh = Channel.createLike[M](inCh)
    
    scheduler execute reset {
      def transfer(from: Channel[M], to: Channel[M]): Unit @suspendable = {
        try while (true)
          to put from.take
        catch { case TerminatedChannel => }
      }
      
      try loop {
        transfer(f( inCh.take ), outCh)
      } catch { case TerminatedChannel => outCh.terminate
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
  def flatMapIter[M](f: A => Iterable[M]): Channel[M] = {
  	val inCh  = this
    val outCh = Channel.createLike[M](inCh)
    
    scheduler execute reset {
      try loop {
        outCh putAll f( inCh.take )
      } catch { case TerminatedChannel => outCh.terminate
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
  def filter(p: A => Boolean @suspendable): Channel[A] = {
    val inCh  = this
    val outCh = Channel.createLike[A](inCh)
    
    scheduler execute { reset {
      try loop {
        val in = inCh.take
        if (p(in)) outCh put in 
        else       cpsunit
      } catch { case TerminatedChannel => outCh.terminate
                case BreakLoop         =>
                case e:Exception       => handleEx(e) }
    }}
    outCh
  }
  
  @deprecated("NOT YET TESTED")
  def withFilter(p: A => Boolean @suspendable): ChannelFilter =
    new ChannelFilter(p)
  
  
  class ChannelFilter(p: A => Boolean @suspendable) {
    
    def foreach[U](f: A => U @suspendable): Unit @suspendable = {
      val ch = self
      try loop {
        val v = ch.take
        if (p(v)) f(v)
        else      cpsunit //XXX U vs. Unit
      } catch { case TerminatedChannel => 
                case BreakLoop         =>
                case e:Exception       => handleEx(e) }
    }
    
    def map[M](f: A => M @suspendable)(implicit scheduler: Scheduler): Channel[M] = {
      val inCh  = self
      val outCh = Channel.createLike[M](inCh)
  
      scheduler execute { reset {
        try loop {
          val v = inCh.take
          if (p(v)) outCh put f(v)
          else      cpsunit
        } catch { case TerminatedChannel => outCh.terminate
                  case BreakLoop         =>
                  case e:Exception       => handleEx(e) }
      }}
      outCh
    }
    
    def flatMap[M](f: A => Channel[M] @suspendable): Channel[M] = {
      val inCh  = self
      val outCh = Channel.createLike[M](inCh)
      
      scheduler execute reset {
        def transfer(from: Channel[M], to: Channel[M]): Unit @suspendable = {
          try while (true)
            to put from.take
          catch { case TerminatedChannel => }
        }
        
        try loop {
          val v = inCh.take
          if (p(v)) transfer(f(v), outCh)
          else      cpsunit
        } catch { case TerminatedChannel => outCh.terminate
                  case BreakLoop         =>
                  case e:Exception       => handleEx(e) }
      }
      outCh
    }
    
    def withFilter(q: A => Boolean): ChannelFilter = 
      new ChannelFilter(x => p(x) && q(x))
  }
  
  
  
  /**
   * applies given binary function f with initial accumulator value z on this channel
   
   * THIS CHANNEL MUST NOT BE READ FROM AFTER USING THIS METHOD 
   * 
   * @return result of reduction 
   */
  def fold[B](z: B)(f: (A,B) => B @suspendable): B @suspendable = {
    var res = z  
    this.foreach(x => {
      res = f(x, res)
      cpsunit
    })
    res
  }
  
  @throws(classOf[TerminatedChannel])
  def reduce[B >: A](f: (A,B) => B @suspendable): B @suspendable = {
    val z: B = this.take
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
  def duplicate(): (Channel[A], Channel[A]) = {
  	val inCh  = this
  	val outCh1 = Channel.createLike[A](this)
  	val outCh2 = Channel.createLike[A](this)
  	
  	scheduler execute { reset {
  	  try loop {
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
  def split(p: A => Boolean @suspendable): (Channel[A], Channel[A]) = {
  	val inCh  = this
  	val outCh1 = Channel.createLike[A](this)
  	val outCh2 = Channel.createLike[A](this)
  	
  	scheduler execute { reset {
  	  try loop {
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
  def combine[B <: A](ch2: ChannelTake[B]): Channel[A] = {
    val ch  = Channel.createLike[A](this)
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
  def concat[B <: A](ch2: ChannelTake[B]): Channel[A] = {
    val ch  = Channel.createLike[A](this)
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
  def transfer[B](to: ChannelPut[B], f: A => B): Unit = {
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
  def transferTerminate[B](to: ChannelPut[B] with ChannelTerminate[B], f: A => B): Unit = {
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
  
  def toList(): List[A] @suspendable = {
    var xs = List[A]()
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
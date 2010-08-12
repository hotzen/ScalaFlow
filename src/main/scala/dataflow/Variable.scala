package dataflow

import scala.util.continuations._
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.ConcurrentLinkedQueue

final class AlreadySetException(msg: String) extends dataflow.DataFlowException(msg)


final class Variable[T](implicit scheduler: Scheduler) extends SuspendingAwait {
	
  type Continuation = (T => Unit)
  
  private[dataflow] val value = new AtomicReference[Option[T]](None)
  private[this] val suspendedReaders = new ConcurrentLinkedQueue[Continuation]
  
  def isSet: Boolean = value.get.isDefined
  
  
  def get: T @suspendable = {
  	val optVal = value.get

    if (optVal.isDefined) {
      optVal.get
    } else {
      shift { k: Continuation =>
        if (value.get.isDefined) {
          //println("__DFVAR-RACE#1__")
        	k( value.get.get )
        } else {
        	suspendedReaders offer k
        	if (value.get.isDefined && suspendedReaders.remove(k)) {
        	  //println("__DFVAR-RACE#2__")
        	  k( value.get.get )
        	}
        }
      }
    }
  }
  def apply(): T @suspendable = get

  
  def tryGet: Option[T] @suspendable = {
    if (!isSet) None
    else        Some( get )
  }
  
    
  def set(v: T): Unit = { // NOT @suspendable
    if (value.compareAndSet(None, Some(v)))
      resumeReaders(v)
    else
      throw new AlreadySetException("Attempt to change DataFlow-Variable (from [" + value.get + "] to [" + Some(v) + "])")
  }
  def := (v: T): Unit = set(v)

  
  def trySet(v: T): Boolean = { // NOT @suspendable
    if (value.compareAndSet(None, Some(v))) {
      resumeReaders(v)
      true
    } else {
      false
    }
  }
  def :=? (v: T): Boolean = trySet(v)

  
  private def resumeReaders(v: T): Unit = {
    while (!suspendedReaders.isEmpty) {
      val k = suspendedReaders.poll
      if (k != null) {
        scheduler execute { k(v) }
      }
    }
  }
   
  def sawait: Boolean @suspendable = { apply(); true }
  
  override def toString = "<Variable "+value.get+">"
}

object Signal {
  
  def combine(ss: Iterable[Signal])(implicit scheduler: Scheduler): Signal = {
    val sig = new Signal
    scheduler execute { reset {
      val it = ss.iterator
      while (it.hasNext)
        it.next.sawait
      sig.invoke
    }}
    sig
  }
}



final class Signal(implicit scheduler: Scheduler) extends SuspendingAwait {
  
  private[this] val sync = new Variable[Boolean]
    
  def sawait: Boolean @suspendable =
    sync.get
    
  def invoke: Unit =
  	sync := true
}
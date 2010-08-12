package dataflow

import scala.util.continuations._

object Flow {
  def create[R](computation: => R @suspendable)(implicit scheduler: Scheduler): Flow[R] =
    new Flow[R](() => computation)
  
  def flow[R](computation: => R @suspendable)(implicit scheduler: Scheduler): FlowResult[R] =
    create[R](computation).execute.result
}

final class Flow[T](val k: () => T @suspendable)(implicit val scheduler: Scheduler) {
  val result = new FlowResult[T]
  
  def execute: Flow[T] = {
    scheduler execute { reset {
      result set k.apply
    }}
    this
  }
  
  override def toString = "<Flow " + result + ">"
}

final class FlowResult[T](implicit val scheduler: Scheduler) extends BlockingAwait with SuspendingAwait {
  import java.util.concurrent.CountDownLatch

  private val dfvar = new Variable[T]
  private val latch = new CountDownLatch(1)
  
  def set(v: T): Unit = {
    dfvar set v
    latch.countDown
  }

  // blocking
  def get: T = {
    latch.await
    dfvar.value.get.get //XXX dirty?
  }

  // blocking await
  def await: Boolean = {
    latch.await
    true
  }

  // suspending
  def sget: T @suspendable =
    dfvar.get
    
  // suspending await
  def sawait: Boolean @suspendable = {
    dfvar.get
    true
  }

  override def toString = "<FlowResult " + dfvar.value.get + ">"
}
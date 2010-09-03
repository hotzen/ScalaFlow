import scala.util.continuations.suspendable

package object dataflow {
  
  type dataflow = suspendable
  
  // exporting the implicit conversion for iterables
  import DataFlowIterable.convert
  
  def flow[R](computation: => R @suspendable)(implicit scheduler: Scheduler): FlowResult[R] =
    Flow.flow(computation)(scheduler)
    
  def loopWhile[U](c: => Boolean)(comp: => U @suspendable): Unit @suspendable = {
    def f(): TailRec[Unit] @suspendable = {
      if (c) {
        comp
        Call( () => f() )
      } else Return( () )
    }
    tailrec( f() )
  }
    
  def loop[U](comp: => U @suspendable): Unit @suspendable = {
    def f(): TailRec[Unit] @suspendable = {
      comp
      Call( () => f() )
    }
    tailrec( f() )
  }
    
  // tail-recursion for dataflow computations using trampolining
  def tailrec[A](comp: TailRec[A]): A @suspendable = comp match {
    case Call(thunk) => tailrec( thunk() )
    case Return(x)   => x
  }
}

package dataflow {
	class DataFlowException(val msg: String, val cause: Throwable) extends RuntimeException(msg, cause) {
	  def this(msg: String) = this(msg, null)
	}
  	
  trait BlockingAwait {
    def await: Boolean
  }
  
  trait SuspendingAwait {
    import scala.util.continuations.suspendable
    def sawait: Boolean @suspendable
  }
    
  sealed trait TailRec[A]
  case class Return[A](result: A) extends TailRec[A]
  case class Call[A](thunk: () => TailRec[A] @suspendable) extends TailRec[A]
}
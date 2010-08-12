//import scala.util.continuations._

package object dataflow {
  
  type dataflow = scala.util.continuations.suspendable
  
  // TODO: needed?
  def dfunit: Unit @dataflow = ()
  //private[dataflow] def dfid[A](a: A): A @dataflow = a
  
  def flow[R](computation: => R @dataflow)(implicit scheduler: Scheduler): FlowResult[R] =
    Flow.flow(computation)(scheduler)
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
}
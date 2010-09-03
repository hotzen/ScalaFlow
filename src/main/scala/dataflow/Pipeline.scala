package dataflow

import scala.util.continuations._

object Pipeline {
}

class Pipeline[A](implicit val scheduler: Scheduler) {
	val inCh = Channel.create[A]
	
	def put(v: A): Unit @suspendable = inCh put v
  def << (v: A): Unit @suspendable = put(v)
		
  def terminate: Unit @suspendable = inCh.terminate
  def <<# : Unit @suspendable = terminate
  
	def stage[B](f: A => B @suspendable): PipelineStage[A,B] =
		new PipelineStage[A,B](inCh, f)
}

class PipelineStage[A,B](val inCh: Channel[A], f: A => B @suspendable)(implicit val scheduler: Scheduler) {
	val outCh: Channel[B] = inCh.map(f)
	
	def stage[C](f: B => C @suspendable) =
		new PipelineStage[B,C](outCh, f)
	
	def foreach(f: B => Unit @suspendable): Unit @suspendable = 
		outCh.foreach(f)
}
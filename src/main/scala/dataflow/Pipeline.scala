package dataflow

import scala.util.continuations._

object Pipeline {

}

class Pipeline[A](implicit val scheduler: Scheduler) {
	
	val inCh = Channel.create[A]
	
	def <<(value: A): Unit @suspendable =
		inCh << value
	
	def <<# : Unit @suspendable =
	  inCh <<#
	
	def next[B](f: A => B @suspendable): PipelineStage[A,B] =
		new PipelineStage[A,B](inCh, f)
}

class PipelineStage[A,B](val inCh: Channel[A], f: A => B @suspendable)(implicit val scheduler: Scheduler) {
	
	val outCh: Channel[B] = inCh.map(f)
	
	def next[C](f: B => C @suspendable) =
		new PipelineStage[B,C](outCh, f)
	
	def foreach(f: B => Unit @suspendable): Unit @suspendable = 
		outCh.foreach(f)
		
	def apply(): B @suspendable =
		outCh.apply()
}
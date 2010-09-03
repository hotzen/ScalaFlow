package dataflow


object Scheduler {
  import java.util.concurrent._

  val MinThreads = 4
  val ShutdownAwaitMaxSeconds = 1
}

trait Scheduler {
  def execute(f: => Unit): Unit
  def shutdown: BlockingAwait
}

class ThreadPoolScheduler extends Scheduler {
  import java.util.concurrent.{Executors, ThreadFactory, ExecutorService, ThreadPoolExecutor, LinkedBlockingQueue}
  import java.util.concurrent.TimeUnit, TimeUnit._

  // http://java.sun.com/j2se/1.5.0/docs/api/java/util/concurrent/ThreadPoolExecutor.html
  def createExecutor(threadFactory: ThreadFactory): ExecutorService = {
    val numCores = Runtime.getRuntime().availableProcessors()
    val coreSize = Scheduler.MinThreads //2 Math.max 2*numCores
    val maxSize  = coreSize // no effect on unbounded LinkedBlockingQueue
    
    val keepAliveTime = Long.MaxValue
    val workQueue     = new LinkedBlockingQueue[Runnable]
    
    val rejectHandler = new ThreadPoolExecutor.AbortPolicy // CallerRunsPolicy
    
    new ThreadPoolExecutor(
      coreSize,
      maxSize,
      keepAliveTime,
      TimeUnit.MILLISECONDS,
      workQueue,
      threadFactory,
      rejectHandler
    )
  }
  
  protected val executor = createExecutor( Executors.defaultThreadFactory() )

  def execute(f: => Unit): Unit = executor execute new Runnable {
    //XXX proper exception-handling
    def run() = try { f } catch { case e:Exception => e.printStackTrace }
    //def run() = { f }
  }
  
  def shutdown = {
    executor.shutdown()
    new BlockingAwait {
      def await = executor.awaitTermination(Scheduler.ShutdownAwaitMaxSeconds, SECONDS)
    }
  }
}


class DaemonThreadPoolScheduler extends ThreadPoolScheduler {
  import java.util.concurrent.ThreadFactory
  
  private val daemonFactory = new ThreadFactory {
    def newThread(r: Runnable): Thread = {
      val t = new Thread(r)
      t setDaemon true
      t
    }
  } 

  override protected val executor = createExecutor( daemonFactory )
  
  override def shutdown = {
    executor.shutdown() // just to be nice
    new BlockingAwait {
      def await = true // daemons get auto-killed
    }
  }
}

class ForkJoinScheduler extends Scheduler {
  import scala.concurrent.forkjoin._
  import java.util.concurrent.TimeUnit._
  
  private val executor = {
    val p = new ForkJoinPool()
    p setParallelism Scheduler.MinThreads
    p setMaintainsParallelism true
    p setAsyncMode true
    p
  }
  
  def execute(f: => Unit): Unit = executor execute new RecursiveAction {
    //XXX proper exception-handling
    def compute() = try { f } catch { case e:Exception => e.printStackTrace }
    //def compute() = { f }
  }
  
  def shutdown = {
    executor.shutdown()
    new BlockingAwait {
      def await = executor.awaitTermination(Scheduler.ShutdownAwaitMaxSeconds, SECONDS)
    }
  }
}
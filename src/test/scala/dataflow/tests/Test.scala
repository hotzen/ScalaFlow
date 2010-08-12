package dataflow
package tests

abstract class Test extends util.Logging {

  val self = this

	def main(args: Array[String]): Unit = run
	def run: Unit
}

abstract class Benchmark extends util.Logging {
  import java.util.concurrent.CountDownLatch
  
  val self = this
  
  val WarmupRuns: Int = 5
  val BenchRuns: Int  = 3
  
  def run(latch: CountDownLatch): Unit

  def main(args: Array[String]): Unit = {
    
    println("# WARMUP")
    runBenchmark( WarmupRuns )
    
    println("# GC")
    System.gc()
    
    println("# BENCHMARKING")
    val times = runBenchmark( BenchRuns )
    val avg = times.foldLeft[Long](0)(_ + _) / times.length
    
    println("### AVG: " + avg + " msecs")
  }
  
  def runBenchmark(runs: Int): List[Long] = {
    (for (i <- 1 to runs) yield {
      print(i + "/" + runs + " ...")
      val latch = newLatch  
      
      val start = millis
      run( latch )
      latch.await
      val dur = millis - start
      
      println(" " + dur + " ms")
      dur
    }).toList
  }
  
  def millis = System.currentTimeMillis
  def newLatch: CountDownLatch = new CountDownLatch(1)
}
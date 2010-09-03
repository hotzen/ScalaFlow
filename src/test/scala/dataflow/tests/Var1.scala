package dataflow
package tests

import dataflow._

object Var1  {

  implicit val scheduler = new DaemonThreadPoolScheduler
  
  def main(args: Array[String]): Unit = {
    val v = new Variable[Int]
    
    val f1 = flow { v()     }
    val f2 = flow { v := 42 }
    
    println("r1="+f1.get+" r2="+f2.get)
  }
}
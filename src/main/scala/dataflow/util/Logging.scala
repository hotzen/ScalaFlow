package dataflow.util

import java.io.PrintStream

trait Logging {
  // XXX check why NPEs with lazy val
  val log = Logger.get(this.getClass.getName)
}

sealed class LogLevel(val name: String, val order: Int)
object LogLevel {
  case object All   extends LogLevel("<All>", Int.MinValue)
  
  case object Trace extends LogLevel("Trace",   100)
  case object Info  extends LogLevel("Info",    200)
  case object Warn  extends LogLevel("Warning", 300)
  case object Error extends LogLevel("ERROR",   400)
  
  case object Off   extends LogLevel("<Off>", Int.MaxValue)
  
  def apply(name: String, order: Int) =
    new LogLevel(name, order)
}	

object Logger {
  var DefaultLevel: LogLevel  = LogLevel.Warn
  var DefaultOut: PrintStream = Console.out
  
	val loggers = scala.collection.mutable.HashMap[String,Logger]()
//  var loggers = List[Logger]()
  
	def get(name: String): Logger = {
		loggers.get(name) match {
	  	case Some(logger) => logger
	  	case None => {
	  		val logger = new Logger(name, DefaultLevel, DefaultOut)
	  		loggers += (name -> logger)
	  		logger
	  	}
	  }
//    val logger = new Logger(name, DefaultLevel, DefaultOut)
//    loggers = logger :: loggers
//    logger
	}
}

class Logger(var name: String, var level: LogLevel, var out: PrintStream) {
	def mustLog(lvl: LogLevel): Boolean = (lvl.order >= level.order)
	
	def log(lvl: LogLevel, msg: String): Unit =
	  if (mustLog(lvl))
		  out.println("["+lvl.name+"] ["+Thread.currentThread.getName+"] <"+name+"> " + msg)		
	
	def trace(msg: => String) =
	  if (mustLog(LogLevel.Trace))
	    log(LogLevel.Trace, msg)
	
	def info(msg: => String) =
	  if (mustLog(LogLevel.Info))
	    log(LogLevel.Info, msg)
	
	def warn(msg: String) =
	  if (mustLog(LogLevel.Warn))
	    log(LogLevel.Warn, msg)

	def error(msg: String) =
	  if (mustLog(LogLevel.Error))
	    log(LogLevel.Error, msg)
	
	def error(e: Throwable, msg: String) =
	  if (mustLog(LogLevel.Error))
		  log(LogLevel.Error, msg + ": " + e.toString + " [" + e.getStackTraceString + "]")
}
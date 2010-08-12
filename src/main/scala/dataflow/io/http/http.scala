package dataflow.io

package object http {
	import java.net.URL
	import java.nio.charset.Charset

	// 2.2 Basic Rules - http://www.w3.org/Protocols/rfc2616/rfc2616-sec2.html#sec2.2
	val CR = 13
	val LF = 10
  val CRLF = "" + CR.toChar + LF.toChar

//  val ASCII   = Charset.forName("US-ASCII")
  val ISO8859 = Charset.forName("ISO-8859-1")
  val UTF8    = Charset.forName("UTF-8")
}
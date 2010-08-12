package dataflow
package io
package http

import java.net.URI
import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset.Charset
import java.lang.CharSequence

// ##########################
// METHOD
// http://www.w3.org/Protocols/rfc2616/rfc2616-sec5.html#sec5

object HttpMethod {
  val OPTIONS = HttpMethod("OPTIONS")
  val GET     = HttpMethod("GET")
  val HEAD    = HttpMethod("HEAD")
  val POST    = HttpMethod("POST")
  val PUT     = HttpMethod("PUT")
  val DELETE  = HttpMethod("DELETE")
  val TRACE   = HttpMethod("TRACE")
  val CONNECT = HttpMethod("CONNECT")
}

case class HttpMethod(cmd: String)


// ##########################
// HEADER

object HttpHeader {
  val UserAgent               = HttpHeader("User-Agent", "Mozilla/5.0")
  def UserAgent(name: String) = HttpHeader("User-Agent", name)
  
  def Referer(uri: String)    = HttpHeader("Referer", uri)
    
  val ConnectionClose         = HttpHeader("Connection", "close")
  val ConnectionKeepAlive     = HttpHeader("Connection", "keep-alive")
  
   
  def isHeaderOfName(name: String)(header: HttpHeader) =
    header.name.compareToIgnoreCase(name) == 0
}

case class HttpHeader(name: String, value: String) {
	import http.CRLF
	
  def appendTo(sb: StringBuilder): StringBuilder =
    sb.append(name).append(": ").append(value).append(CRLF)
}



// ##########################
// ContentType

object HttpContentType {
  val XML   = HttpContentType("text/xml")
  val XHTML = HttpContentType("application/xhtml+xml")
  val HTML  = HttpContentType("text/html")
  val JSON  = HttpContentType("application/json")
  val JS    = HttpContentType("text/javascript") // e.g. Google Ajax API
  
  // group(1) = Media-Type
  // group(2) = charset or null
  def regex: scala.util.matching.Regex =
    """(\w+/\w+)(?:[ ]*;[ ]*charset=(.+))?""".r
}

case class HttpContentType(mediaType: String) {

  def toHeader: HttpHeader =
    HttpHeader("Content-Type", mediaType)
  
  def toHeader(charset: String): HttpHeader =
    HttpHeader("Content-Type", mediaType + ";charset=" + charset)
}



// ##########################
// REQUEST

object HttpRequest {
  def GET(uri: URI)    = new HttpRequest(HttpMethod.GET,    uri, Nil)
  def POST(uri: URI)   = new HttpRequest(HttpMethod.POST,   uri, Nil)
  def PUT(uri: URI)    = new HttpRequest(HttpMethod.PUT,    uri, Nil)
  def DELETE(uri: URI) = new HttpRequest(HttpMethod.DELETE, uri, Nil)
  def HEAD(uri: URI)   = new HttpRequest(HttpMethod.HEAD,   uri, Nil)
  
  def encode(s: String): String = java.net.URLEncoder.encode(s, "UTF-8")
  
  val DefaultPort = 80
}

case class HttpRequest(method: HttpMethod, uri: URI, headers: List[HttpHeader], httpVersion:(Int,Int) = (1,1)) {
    
  def host: String = uri.getHost
  def port: Int = {
  	val p = uri.getPort
  	if (p > 0) p
  	else       HttpRequest.DefaultPort
  }
  
  def withHeader(header: HttpHeader): HttpRequest =
    HttpRequest(method, uri, header :: headers)
  
  def withHeader(name: String, value: String): HttpRequest =
    withHeader( HttpHeader(name, value) )
  	
  def withConnectionClose: HttpRequest =
    withHeader( HttpHeader.ConnectionClose )
    
  def withConnectionKeepAlive: HttpRequest =
    withHeader( HttpHeader.ConnectionKeepAlive )
        
  def toBytes: ByteBuffer = {
  	
    val path =
      if ((uri.getPath ne null) && (uri.getPath.length > 0))
        uri.getPath
      else
        "/"

    var query =
      if ((uri.getQuery ne null) && (uri.getQuery.length > 0))
        "?" + uri.getQuery
      else
        ""
    
    val sb = new StringBuilder
    sb.append(method.cmd).append(" ")
      .append(path).append(query).append(" ")
      .append("HTTP/").append(httpVersion._1).append(".").append(httpVersion._2)
      .append(http.CRLF)
  	HttpHeader("Host", uri.getHost).appendTo(sb)
  	    
  	for (header <- headers.reverse)
  	  header.appendTo(sb)
  	
    sb.append(CRLF)
  	ISO8859.encode( CharBuffer.wrap(sb.toString) )
  }
}


// ##########################
// RESPONSE

case class HttpResponse(status: HttpResponseStatus, headers: List[HttpHeader]) {
	  
  private val bodyBuf = new StringBuilder
  lazy val body = bodyBuf.toString
    
  def isOK = (status == HttpResponseStatus.OK)
    
  def addBody(cs: CharSequence): HttpResponse = {
    bodyBuf append cs
    this
  }
  
  def toBytes: ByteBuffer = {
    val sb = new StringBuilder
    sb.append("HTTP/").append(status.httpVersion._1).append(".").append(status.httpVersion._2).append(" ")
      .append(status.code).append(" ")
      .append(status.phrase).append(http.CRLF)
    
    def addHeadersWithContentLength(length: Int) = {
      val isContentLength = HttpHeader.isHeaderOfName("Content-Length") _
      headers.filter(!isContentLength(_)).foreach(_.appendTo(sb))
      HttpHeader("Content-Length", length.toString).appendTo(sb)
    }
    
    val bCR = http.CR.toByte
    val bLF = http.LF.toByte

    if (body.length > 0) {
      val bodyBuf = UTF8.encode(CharBuffer.wrap( body ))
      val bodyLen = bodyBuf.remaining
      
      addHeadersWithContentLength(bodyLen)
      val headerBuf = ISO8859.encode(CharBuffer.wrap( sb.toString ))
      val headerLen = headerBuf.remaining
    
      val buf = ByteBuffer.allocate(headerLen + 2 + bodyLen + 2)
      buf.put(headerBuf).put( bCR ).put( bLF )
         .put(bodyBuf).put( bCR ).put( bLF )
      buf.flip
      buf
    } else {
      addHeadersWithContentLength(0)
      val headerBuf = ISO8859.encode(CharBuffer.wrap( sb.toString ))
      val headerLen = headerBuf.remaining
       
      val buf = ByteBuffer.allocate(headerLen + 2)
      buf.put(headerBuf).put( bCR ).put( bLF )
      buf.flip
      buf
    }
  }
  
	override def toString = {
	  val sb = new StringBuilder
	  sb.append("HttpResponse(\n")
	    .append("  Status: ").append( status.toString ).append("\n")
	    .append("  Headers:\n")
	  headers.foreach(h => sb.append("    ").append(h.toString).append("\n") )
	  sb.append ("  Body: <omitted>\n")
	    .append (")")
	  sb.toString
	}
}


// ##########################
// RESPONSE-STATUS

object HttpResponseStatus {
  
  val Continue            = HttpResponseStatus(100, "Continue")
  
  val OK                  = HttpResponseStatus(200, "OK")
  val NoContent           = HttpResponseStatus(204, "No Content")
  
  val MovedPermanently    = HttpResponseStatus(301, "Moved Permanently")
  val Found               = HttpResponseStatus(302, "Found")
  val SeeOther            = HttpResponseStatus(303, "See Other")
  val NotModified         = HttpResponseStatus(304, "Not Modified")
  
  val BadRequest          = HttpResponseStatus(400, "Bad Request")
  val Unauthorized        = HttpResponseStatus(401, "Unauthorized")
  val Forbidden           = HttpResponseStatus(403, "Forbidden")
  val NotFound            = HttpResponseStatus(404, "Not Found")
  
  val InternalServerError = HttpResponseStatus(500, "Internal Server Error")
  
  sealed trait HttpResponseStatusType
    object Informational extends HttpResponseStatusType {
      def unapply(status: HttpResponseStatus): Boolean = status.isInformational
    }
    object Success extends HttpResponseStatusType {
      def unapply(status: HttpResponseStatus): Boolean = status.isSuccess
    }
    object Redirection extends HttpResponseStatusType {
      def unapply(status: HttpResponseStatus): Boolean = status.isRedirection
    }
    object ClientError extends HttpResponseStatusType {
      def unapply(status: HttpResponseStatus): Boolean = status.isClientError
    }
    object ServerError extends HttpResponseStatusType {
      def unapply(status: HttpResponseStatus): Boolean = status.isServerError
    }
}

case class HttpResponseStatus(code: Int, phrase: String, httpVersion: (Int,Int) = (1,1)) {
  def isInformational = code / 100 == 1
  def isSuccess       = code / 100 == 2
  def isRedirection   = code / 100 == 3
  def isClientError   = code / 100 == 4
  def isServerError   = code / 100 == 5
}
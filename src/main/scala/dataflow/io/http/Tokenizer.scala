package dataflow.io
package http

import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset.{Charset, CharsetDecoder, CodingErrorAction}

object Tokenizer {
   
  case object IsResponse {
    def unapply(s: String): Boolean =
      s.startsWith("HTTP")
  }
  
  case object IsRequest {
    val ms = List(
      HttpMethod.OPTIONS,
      HttpMethod.GET,
      HttpMethod.HEAD,
      HttpMethod.POST,
      HttpMethod.PUT,
      HttpMethod.DELETE,
      HttpMethod.TRACE,
      HttpMethod.CONNECT
    )
    def unapply(s: String): Boolean =
      ms.exists(m => s.startsWith(m.cmd))
  }
  
  val InitCheckLength = 10
  
  val RequestMustOccurWithin = 4000 // URIs might get large, apache-like behaviour
  val StatusMustOccurWithin  = 200
  
  // 5.1 Request-Line - http://www.w3.org/Protocols/rfc2616/rfc2616-sec5.html#sec5.1
  // 3.1 HTTP Version - http://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.1
  val RequestRegex = """([A-Z]+) ([^ ]+) HTTP/(\d)\.(\d)""".r
  
  // 6.1 Status-Line - http://www.w3.org/Protocols/rfc2616/rfc2616-sec6.html#sec6.1
  // 3.1 HTTP Version - http://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.1
  val StatusRegex = """HTTP/(\d)\.(\d) (\d{3}) (.+)""".r
  
  // 4.2 Message Headers - http://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html#sec4.2
  val HeaderRegex = """([\w-]+)[ ]*:[ ]*(.+)""".r
  
  // 3.6.1 Chunked Transfer Coding - http://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.6.1
  // XXX trailing spaces WTF 
  // TODO chunk-extension
  val ChunkRegex  = """([0-9A-Fa-f]+)[ ]*""".r
	
  val IsContentLengthHeader    = HttpHeader.isHeaderOfName("Content-Length") _
  val IsTransferEncodingHeader = HttpHeader.isHeaderOfName("Transfer-Encoding") _

  sealed trait State
  object State {
    case object Init extends State
    
    case object Request extends State
    case object Status extends State
    
    case object InitRequestHeaders  extends State
    case object InitResponseHeaders extends State
        
    case class Headers(headers: List[HttpHeader], isRequest: Boolean) extends State
            
    // 4.4 Message Length - http://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html#sec4.4
    case class InitBody(headers: List[HttpHeader]) extends State
    case class Body(size: Int) extends State
    
    case object Stream extends State
        
    // 3.6.1 Chunked Transfer Coding - http://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.6.1
    case object InitChunk extends State
    case class Chunk(size: Int) extends State
    case class RemoveChunkBoundary(nextState: State) extends State
    
    case object Success extends State
    case class Failure(cause: String) extends State
  }
  
  sealed trait Token
    case class Request(request: HttpRequest) extends Token  
    case class Status(status: HttpResponseStatus) extends Token
    
    case object StartHeaders extends Token
	  case class Header(header: HttpHeader) extends Token
	  case object EndHeaders extends Token
	  
	  case class Body(bytes: Array[Byte])  extends Token
	  
	  case object StartStream extends Token
	  case class Stream(bytes: Array[Byte]) extends Token
	  
	  case object StartChunks extends Token
	  case class Chunk(bytes: Array[Byte]) extends Token
	  case object EndChunks extends Token
}

class Tokenizer extends dataflow.util.Logging {
  import Tokenizer._
  import http.{CR,LF, ISO8859} //XXX why explicit?
   
  type TokenizeResult = (Boolean, State, Option[Token])
    
  private var state: State = State.Init
  private val bytes = new scala.collection.mutable.ArrayBuffer[Byte]( Socket.ReadBufferSize )
  
  def currentState: State = state
  def currentBuffer: Array[Byte] = bytes.toArray
  
  val httpDecoder = {
    val dec = ISO8859.newDecoder
    dec onMalformedInput      CodingErrorAction.REPORT //XXX change to REPLACE
    dec onUnmappableCharacter CodingErrorAction.REPORT
    dec
  }
  
  def reset {
    state = State.Init
    bytes.clear()
  }
  
  def tokenize(bs: Array[Byte]): List[Token] = {
    bytes ++= bs
    
    var tokens   = List[Token]()
    var continue = true
    while (continue) {
      continue = tokenizeNext match {
        //TODO check
        case (_, State.Failure(cause), _) =>
          throw new Exception("Failure: " + cause)
        
        case (c, s, None) => {
          state = s
          c
        }
        case (c, s, Some(t)) => {
          state = s
          tokens ::= t
          c
        }
      }
      //log trace ("tokenize: new-state=" + state + " continue=" + continue)
    }
    tokens.reverse
  }
    
  def continue(s: State): TokenizeResult =
    (true, s, None)
  def continue(s: State, t: Token): TokenizeResult =
    (true, s, Some(t))

  def suspend(s: State): TokenizeResult =
    (false, s, None)
  def suspend(s: State, t: Token): TokenizeResult =
    (false, s, Some(t))

  def fail(cause: String): TokenizeResult =
    (false, State.Failure(cause), None)
  
  def tokenizeNext: TokenizeResult = state match {
    
    case State.Init => {
      if (bytes.length < InitCheckLength) {
        log trace ("State.Init: bytes=" + bytes.length + " < InitCheckLength=" + InitCheckLength + ", suspending")
        suspend( State.Init )
      }
      else {
        decode(0, InitCheckLength) match {
          case IsRequest() => {
            log trace ("State.Init: is request")
            continue( State.Request )
          }  
          case IsResponse() => {
            log trace ("State.Init: is response")
            continue( State.Status )
          }
          case x => 
            fail("State.Init: invalid prefix: " + debug(x))
        }
      }
    }
    
    // http://www.w3.org/Protocols/rfc2616/rfc2616-sec5.html#sec5
    case State.Request => findCRLF match {
      case Some(pos) => decode(0, pos) match {
        case RequestRegex(rawMethod, rawUri, httpMajor, httpMinor) => {
          import java.net.URI
          val method = HttpMethod(rawMethod)
          
          if (rawUri.isEmpty)
            return fail("State.Request: empty request-URI")
          
          val uri = try {
            if (rawUri.startsWith("/"))
              new URI("http://_host_" + rawUri) // host-header not parsed yet
            else
              new URI(rawUri)
          } catch { case e:Exception =>
            return fail("State.Request: invalid request-URI: " + debug(rawUri))
          }
          
          val httpVersion = (httpMajor.toInt, httpMinor.toInt)
          val request = HttpRequest(method, uri, Nil, httpVersion)
          log trace ("State.Request: " + request)
          
          bytes.remove(0, pos+2)
          continue( State.InitRequestHeaders, Request(request) )
        }
        case str => fail("State.Request: invalid request: " + debug(str))
      }
      case None => {
        if (bytes.length > RequestMustOccurWithin)
          fail("State.Status: does not contain HTTP-Status: " + debug(decode(0, bytes.length)))
        else
          suspend( State.Status )
      }
    }
    
    // http://www.w3.org/Protocols/rfc2616/rfc2616-sec6.html#sec6
    case State.Status => findCRLF match {
      case Some(pos) => decode(0, pos) match {
        case StatusRegex(httpMajor, httpMinor, statusCode, statusPhrase) => {
          val httpVersion = (httpMajor.toInt, httpMinor.toInt)
          val status = HttpResponseStatus(statusCode.toInt, statusPhrase, httpVersion)
          log trace ("State.Status: " + status)
          bytes.remove(0, pos+2)
          continue( State.InitResponseHeaders, Status(status) )
        }
        case str => fail("State.Status: invalid status: " + debug(str))
      }
      case None => {
        if (bytes.length > StatusMustOccurWithin)
          fail("State.Status: does not contain HTTP-Status: " + debug(decode(0, bytes.length)))
        else
          suspend( State.Status )
      }
    }

    case State.InitRequestHeaders =>
      continue(State.Headers(Nil, true), StartHeaders)
      
    case State.InitResponseHeaders =>
      continue(State.Headers(Nil, false), StartHeaders)  
    
    case State.Headers(headers, isRequest) => findCRLF match {
      case Some(pos) => decode(0, pos) match {
        case "" => {
          log trace ("State.Headers: header-body-separator CRLF")
          bytes.remove(0, pos+2)
          
          if (isRequest)
            getContentLength(headers) match {
              case Some(size) => {
                log trace ("State.Headers: Request-Headers with Content-Length -> POST/PUT data")
                continue( State.Body(size), EndHeaders )
              }
              case None => {
                log trace ("State.Headers: Request-Headers without Content-Length, successfully finished")
                continue( State.Success, EndHeaders )
              }
            }
          else {
            log trace ("State.Headers: Response-Headers, initiating Body")
            continue( State.InitBody(headers), EndHeaders )
          }
        }
        case HeaderRegex(name, value) => {
          val header = HttpHeader(name, value)
          log trace ("State.Headers: " + header)
          bytes.remove(0, pos+2)
          continue( State.Headers(header :: headers, isRequest), Header(header) )
        }
        case str => fail("State.Headers: neither header nor header-body-separator CRLF: " + debug(str))
      }
      case None => suspend( State.Headers(headers, isRequest) )
    }
        
    
    case State.InitBody(headers) => {
      if (isChunked( headers )) {
        log trace ("State.InitBody: chunked")
        continue( State.InitChunk, StartChunks )
      } else {
        getContentLength( headers ) match {
          case Some(size) => {
            log trace ("State.InitBody: size=" + size)
            continue( State.Body(size) )
          }
          case None => {
            log trace ("State.InitBody: stream")
            continue( State.Stream, StartStream )
          }
        }
      }
    }
    
    case State.Stream => {
      log trace ("State.Stream")
      bytes.clear
      suspend( State.Stream, Stream( bytes.toArray ) )
    }
    
    case State.Body(size) => {
      val bytesLen = bytes.length
            
      if (bytesLen < size) {
        log trace ("State.Body: bytes=" + bytesLen + " < size=" + size + ": suspending")
        suspend( State.Body(size) )
      }
      else if (bytesLen == size) {
        log trace ("State.Body: bytes=" + bytesLen + " == size=" + size + ": continue with State.Success")
        bytes.clear
        continue( State.Success, Body(bytes.toArray) )
      }
      
      else { // >
        log trace ("State.Body: bytes=" + bytesLen + " > size=" + size + ": taking " + size + " bytes, continue with State.Success")
        val bs = new Array[Byte](size)
        bytes.copyToArray(bs, 0, size)
        bytes.remove(0, size)
        continue( State.Success, Body(bs) )
      }
    }
    
    case State.InitChunk => findCRLF match {
      case Some(pos) => decode(0, pos) match {
        case ChunkRegex(hexSize) => {
          val size = Integer.parseInt(hexSize, 16)
          log trace ("State.InitChunk: 0x" + hexSize + " = " + size + " bytes")
          bytes.remove(0, pos+2)
          continue( State.Chunk(size) )
        }
        case str => fail("State.InitChunk: invalid chunk-token: " + debug(str))
      }
      // no CRLF
      case None => suspend( State.InitChunk )
    }
    
    case State.Chunk(size) => {
      val bytesLen = bytes.length

      if (size == 0) {
        log trace ("State.Chunk: terminal size-0 chunk")
        continue( State.RemoveChunkBoundary(State.Success), EndChunks )
      }
      else if (bytesLen < size) {
        log trace ("State.Chunk: bytes=" + bytesLen + " < size=" + size + ": suspending")
        suspend( State.Chunk(size) )
      }
      else if (bytesLen == size) {
        log trace ("State.Chunk: bytes=" + bytesLen + " == size=" + size + ": taking all")
        val bs = bytes.toArray
        bytes.clear
        continue( State.RemoveChunkBoundary(State.InitChunk), Chunk(bs) )
      }
      else { // >
        log trace ("State.Chunk: bytes=" + bytesLen + " > size=" + size + ": taking " + size + " bytes")
        val bs = new Array[Byte](size)
        bytes.copyToArray(bs, 0, size)
        bytes.remove(0, size)
        continue( State.RemoveChunkBoundary(State.InitChunk), Chunk(bs) )
      }
    }
    
    case State.RemoveChunkBoundary(nextState) => {
      if (bytes.length >= 2) {
        findCRLF match {
          case Some(pos) if pos == 0 => {
            log trace ("State.RemoveChunkBoundary: removing CRLF-boundary")
            bytes.remove(0, 2)
          }
          case _ => {
            log warn ("State.RemoveChunkBoundary: NO trailing CRLF-boundary")
          }
        }
        continue( nextState )
      } else {
        suspend( State.RemoveChunkBoundary(nextState) )
      }
    }
    
        
    // restart tokenizing
    case State.Success => {
      if (bytes.length > 0)
        log warn ("State.Success: untokenized bytes left: " + bytes.length)
      
      log trace ("State.Success: restarting with State.Init")
      suspend( State.Init )
    }
    
    // stay in this state
    // TODO: recovery?
    case State.Failure(cause) => suspend( State.Failure(cause) )
  }
     
  private def findCRLF: Option[Int] = {
    val pos = bytes.indexOf(CR)
    if (pos == -1) None
    else
      if (pos+1 < bytes.length && bytes(pos+1) == LF) Some(pos)
      else None
  }
  
  
  private def decode(from: Int, until: Int): String =
    httpDecoder.decode(
      ByteBuffer.wrap(
        bytes.view(from, until).toArray
      )
    ).toString
  
  def getContentLength(headers: List[HttpHeader]): Option[Int] = {
    headers.find(IsContentLengthHeader) match {
      case Some(header) => {
        try   { Some( header.value.toInt ) }
        catch { case _ => None }
      }
      case None => None
    }
  }
  
  def isChunked(headers: List[HttpHeader]): Boolean = {
    headers.find(IsTransferEncodingHeader) match {
      case Some(header) => (header.value.toLowerCase == "chunked")
      case None         => false
    }
  }
  
  def debug(str: String): String = {
    def explicitSpecials(str: String): String = {
      val sb = new StringBuilder
      for (c <- str) sb append {
        if      (c == CR) "\\r"
        else if (c == LF) "\\n"
        else if (c.isControl) "\\" + c.toByte 
        else c.toString
      }
      sb.toString
    }
    "'" + explicitSpecials(str) + "'(#"+str.length+")"
  }
}
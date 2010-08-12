package dataflow
package io
package http

import scala.util.continuations._
import Tokenizer._

import java.net.URL
import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset.{Charset, CharsetDecoder, CodingErrorAction}

class HttpProcessor(val socket: Socket, val forceCharset: Option[Charset] = None) extends util.Logging {
  
  implicit val dispatcher = socket.dispatcher
  implicit val scheduler  = socket.scheduler
  
  def request(req: HttpRequest): Unit @suspendable = 
    socket.write << Socket.ByteBufferToArray( req.toBytes ) 
  
  def requests: ChannelGet[HttpRequest]   = chRequests
  def responses: ChannelGet[HttpResponse] = chResponses
  
  private lazy val chTokens: Channel[Tokenizer.Token] = {
    val t = new Tokenizer
    log trace ("tokenizing into chTokens")
    
    socket.read flatMapIter { bytes => {
      try   { t.tokenize(bytes) }
      catch { case e:Exception => {
        log error (e, "error while tokenizing bytes of size " + bytes.length + " - " +
                      " tokenizer-state: " + t.currentState + 
                      " tokenizer-buffer-size: " + t.currentBuffer.size)
        Nil
      }}
    }}
  }
  
  private lazy val chRequests: Channel[HttpRequest] = {
    var request: HttpRequest = null
    var host: String = null
    var charset: Charset        = null
    var decoder: CharsetDecoder = null
    
    val defaultCharset = http.ISO8859
    
    val IsHostHeader = HttpHeader.isHeaderOfName("Host") _
    
    object HostHeader {
      def unapply(h: HttpHeader): Option[(String,Int)] = {
        if (IsHostHeader(h)) {
          val hp = h.value.split(":")
          if (hp.length == 1)
            Some( (hp(0), HttpRequest.DefaultPort) )
          else if (hp.length == 2 && hp(1).forall(_.isDigit))
            Some( (hp(0), hp(1).toInt) )
          else {
            log warn ("invalid Host-Header: " + h)
            None
          }
        } else None
      }
    }
    
    def decode(bytes: Array[Byte]): String =
      java.net.URLDecoder.decode(
        decoder.decode( ByteBuffer.wrap(bytes) ).toString,
        charset.name
      )
    
    def create(t: Token): Option[HttpRequest] = t match {
      case Request(r) => {
        request = r
        None
      }
      case StartHeaders =>
        None
      case Header(h) => {
        request = request.withHeader( h )
        
        h match {
          case HostHeader(host, port) => {
            val uri    = request.uri
            val newUri = new java.net.URI(
              uri.getScheme,
              uri.getUserInfo,
              host,
              port,
              uri.getPath,
              uri.getQuery,
              uri.getFragment
            )
            request = request.copy(uri = newUri)
          }
          case _ =>
        }
        
        None
      }
      case EndHeaders => {
        val r = request.copy(headers = request.headers.reverse)
        Some(r)
      }
        
      case Body(bytes) => {
        charset = getCharset(forceCharset, request.headers, defaultCharset)
        decoder = createDecoder( charset )
        
        // TODO what to do with POST data?
        val data = decode( bytes )
        log info ("Request-data: " + data)        
        
        None
      }
      case t => {
        log warn ("chRequests: ignoring token " + t)
        None
      }
    }
    
    def tryCreate(t: Token): Option[HttpRequest] =
      try { create( t ) }
      catch { case e:Exception => {
        log error (e, "error while creating request from token: " + t)
        None
      }}

    log trace ("processing tokens into chRequests")
    chTokens collect (tryCreate _)
  }
  
  private lazy val chResponses: Channel[HttpResponse] = {
    var response: HttpResponse     = null
    var status: HttpResponseStatus = null
    var headers: List[HttpHeader]  = Nil
    var charset: Charset           = null
    var decoder: CharsetDecoder    = null
    
    val defaultCharset = http.ISO8859
    
    def decode(bytes: Array[Byte]): CharBuffer =
      decoder.decode( ByteBuffer.wrap(bytes) )
    
    def create(t: Token): Option[HttpResponse] = t match {
      case Status(s) => {
        status = s
        None
      }
      case StartHeaders => {
        headers = Nil
        None
      }
      case Header(h) => {
        headers ::= h
        None
      }
      case EndHeaders => {
        headers = headers.reverse
        None
      }
      case Body(bytes) => {
        charset = getCharset(forceCharset, headers, defaultCharset)
        decoder = createDecoder( charset )
        response = HttpResponse(status, headers)
        response addBody decode(bytes)
        Some(response)
      }
      case StartStream => {
        charset = getCharset(forceCharset, headers, defaultCharset)
        decoder = createDecoder( charset )
        response = HttpResponse(status, headers)
        None
      }
      case Stream(bytes)  => {
        response addBody decode(bytes)
        //TODO how to detect EndOfStream?
        None
      }
      case StartChunks => {
        charset = getCharset(forceCharset, headers, defaultCharset)
        decoder = createDecoder( charset )
        response = HttpResponse(status, headers)
        None
      }
      case Chunk(bytes) => {
        response addBody decode(bytes)
        None
      }
      case EndChunks =>
        Some(response)
      
      case t => {
        log warn ("chResponses: ignoring token " + t)
        None
      }
    }
    
    def tryCreate(t: Token): Option[HttpResponse] =
      try { create( t ) }
      catch { case e:Exception => {
        log error (e, "error while creating response from token: " + t)
        
        val errStatusCode = 600
        val errStatusPhrase = "dataflow.io.http.HttpProcessor-Error: " + e.getMessage +
                              " (Actual HttpResponseStatus:" + status + ")"

        Some( HttpResponse(HttpResponseStatus(errStatusCode, errStatusPhrase), headers) )
      }}

    log trace ("processing tokens into chResponses")
    chTokens collect (tryCreate _)  
  }
  
  
  
  def loadCharset(name: String): Option[Charset] =
    if (name eq null) None
    else
      try   { Some( Charset.forName(name) ) }
      catch { case _ => {
        log warn ("could not load charset " + name)
        None
      }}
  
  def getCharset(headers: List[HttpHeader]): Option[Charset] =
    headers.find( HttpHeader.isHeaderOfName("Content-Type") ) match {
      case Some(header) => header.value match {
        case HttpContentType.regex(_, csName) => {
          log trace ("getCharset: loading charset specified by ContentType-header: " + header)
          loadCharset(csName)
        }
        case hdr => {
          log trace ("getCharset: invalid ContentType-header: " + hdr) 
          None
        }
      }
      case None => {
        log trace ("getCharset: no ContentType-header")
        None
      }
    }
  
  def getCharset(forced: Option[Charset], headers: List[HttpHeader], default: Charset): Charset =
    forced match {
      case Some(cs) => cs
      case None => {
        getCharset(headers) match {
          case Some(cs) => cs
          case None     => default
        }
      }
    }
  
  def createDecoder(cs: Charset) = {
    val dec = cs.newDecoder
    dec onMalformedInput      CodingErrorAction.REPORT
    dec onUnmappableCharacter CodingErrorAction.REPORT
    dec
  }
}

package dataflow
package io

import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset.Charset

trait SocketReadUtils { self: Socket =>

	def readStrings(implicit charset: Charset): Channel[String] = {
		read.map[String](bytes => {
			charset.decode( ByteBuffer.wrap(bytes) ).toString
		})
	}
	
	def readLines(implicit charset: Charset): Channel[String] = {
		import scala.math.{min, max}
		val CR = 13
		val LF = 10
		val lineBuffer = new StringBuilder

		def toLines(str: String): Iterable[String] = {
			val len = str.length
			var revLines = List[String]()
			var i = 0

			while (i < len) {
				val char = str.charAt(i)
				if (char == LF || char == CR) {
					revLines = lineBuffer.toString :: revLines
					lineBuffer.clear

					// check for \r\n, skip \n
					if (char == CR && (i+1) < len && str.charAt(i+1) == LF) {
						i = i +1
					}
				} else {
					lineBuffer.append(char)
				}
				i = i + 1
			}
			
			revLines.reverse
		}
		
		readStrings.flatMapIter[String](toLines _)
	}
}

trait SocketWriteUtils { self: Socket =>
	
	def writeChars: Channel[Array[Char]] = {
		//TODO writeChars
		Channel.create[Array[Char]]
	}
}

		
	
	
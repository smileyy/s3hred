package smileyy.s3hred.util.io

import java.io.{DataInputStream, DataOutputStream, InputStream, OutputStream}

/**
  * Adds utility to [[InputStream]]s and [[OutputStream]]s
  */
object EnhancedStreams {
  implicit class EnhancedOutputStream[T <: OutputStream](out: T) {
    val datastream = out match {
      case d: DataOutputStream => d
      case o: OutputStream => new DataOutputStream(o)
    }

    def writeInt(i: Int): T = {
      datastream.writeInt(i)
      out
    }

    def writeLong(l: Long): T = {
      datastream.writeLong(l)
      out
    }

    /**
      * Writes a length-prefixed series of bytes
      */
    def writeLengthValue(bytes: Array[Byte]): T = {
      datastream.writeInt(bytes.length)
      datastream.write(bytes)
      out
    }
  }

  implicit class EnhancedInputStream[T <: InputStream](in : T) {
    val datastream = in match {
      case d: DataInputStream => d
      case i: InputStream => new DataInputStream(i)
    }

    def readInt(): Int = datastream.readInt()
    def readLong(): Long = datastream.readLong()

    def readLengthValue(): Array[Byte] = {
      val length = datastream.readInt()
      val bytes = new Array[Byte](length)
      datastream.read(bytes)
      bytes
    }
  }
}

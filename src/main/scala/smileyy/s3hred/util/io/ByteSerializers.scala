package smileyy.s3hred.util.io

import java.nio.charset.StandardCharsets

/**
  * Commonly used serializers for non-primitive types.
  */
object ByteSerializers {
  def serializeValue(v: Any): Array[Byte] = v match {
    case s: String => s.getBytes(StandardCharsets.UTF_8)
    case _ => throw new SerializationException(s"Don't know how to serialize a ${v.getClass}: $v")
  }

  def deserializeValue(bytes: Array[Byte]) = {
    // TODO: use type hints to determine type
    new String(bytes, StandardCharsets.UTF_8)
  }
}

class SerializationException(msg: String, cause: Throwable = null) extends RuntimeException(msg, cause)
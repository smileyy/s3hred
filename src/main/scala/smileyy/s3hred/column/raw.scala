package smileyy.s3hred.column

import java.io.{DataInputStream, DataOutputStream, InputStream, OutputStream}

import smileyy.s3hred.util.io.{ByteSerializers, EnhancedStreams}

/**
  * A "raw" representation of column values
  */
object Raw extends ByteSerialization {
  override def reader(in: DataInputStream): ColumnReader = new RawColumnReader(in)
  override def writer: ColumnWriter = RawColumnWriter
}

class RawColumnReader(in: InputStream) extends ColumnReader {
  import EnhancedStreams._

  var currentValue: Any = null

  override def nextRow(): Unit = {
    currentValue = ByteSerializers.deserializeValue(in.readLengthValue())
  }

  override def eq(value: Any): (() =>  Boolean) = () => value == currentValue
  override def in(values: Set[Any]): (() => Boolean) = () => values.contains(currentValue)
}

object RawColumnWriter extends ColumnWriter {
  import EnhancedStreams._

  override def writeValue(out: DataOutputStream, value: Any): Unit = {
    out.writeLengthValue(ByteSerializers.serializeValue(value))
  }

  override def writeMetadata(out: DataOutputStream): Unit = {}
}
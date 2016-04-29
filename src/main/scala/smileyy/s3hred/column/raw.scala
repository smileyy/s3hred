package smileyy.s3hred.column

import java.io.{DataInputStream, DataOutputStream}

import smileyy.s3hred.util.io.{ByteSerializers, EnhancedStreams}

/**
  * A "raw" representation of column values
  */
object Raw extends ByteSerialization {
  override def reader(data: DataInputStream, meta: DataInputStream): ColumnReader = new RawReader(data, meta)
  override def writer(data: DataOutputStream, meta: DataOutputStream): ColumnWriter = new RawWriter(data, meta)
}

private class RawReader(data: DataInputStream, meta: DataInputStream) extends ColumnReader {
  import EnhancedStreams._

  var currentValue: Any = null

  override def nextRow(): Unit = {
    currentValue = ByteSerializers.deserializeValue(data.readLengthValue())
  }

  override def eq(value: Any): (() =>  Boolean) = () => value == currentValue
  override def in(values: Set[Any]): (() => Boolean) = () => values.contains(currentValue)
}

private class RawWriter(data: DataOutputStream, meta: DataOutputStream) extends ColumnWriter {
  import EnhancedStreams._

  override def write(value: Any): Unit = {
    data.writeLengthValue(ByteSerializers.serializeValue(value))
  }

  override def close(): Unit = {}
}
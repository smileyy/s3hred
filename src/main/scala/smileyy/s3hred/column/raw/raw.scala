package smileyy.s3hred.column.raw

import java.io.{InputStream, OutputStream}

import smileyy.s3hred.column.{ColumnReader, ColumnWriter, Representation}
import smileyy.s3hred.util.io.ByteSerializers
import smileyy.s3hred.util.io.EnhancedStreams

/**
  * A "raw" representation of column values
  */
object Raw extends Representation {
  override def reader(name: String, in: InputStream): ColumnReader = new RawColumnReader(name, in)

  override def writer: ColumnWriter = RawColumnWriter
}

class RawColumnReader(val name: String, in: InputStream) extends ColumnReader {
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

  override def writeValue(out: OutputStream, value: Any): Unit = {
    out.writeLengthValue(ByteSerializers.serializeValue(value))
  }

  override def writeMetadata(out: OutputStream): Unit = {}
}
package smileyy.s3hred.column

import java.io.{InputStream, OutputStream}

class Column(val name: String, val serialization: ColumnSerialization) {
  def reader(in: InputStream): ColumnReader = serialization.reader(name, in)
  def writer: ColumnWriter = serialization.writer

  override def toString: String = s"$name -> $serialization"
}
object Column {
  def apply(name: String, serializataion: ColumnSerialization): Column = new Column(name, serializataion)
}

trait ColumnReader {
  def name: String
  def nextRow(): Unit
  def currentValue: Any

  def eq(value: Any): (() => Boolean)
  def in(values: Set[Any]): (() => Boolean)
}

trait ColumnWriter {
  def writeValue(out: OutputStream, value: Any): Unit
  def writeMetadata(out: OutputStream): Unit
}

package smileyy.s3hred.column

import java.io.{DataInputStream, DataOutputStream, InputStream}

class Column private (val name: String, val serialization: ColumnSerialization) {
  def reader(in: InputStream): ColumnReader = serialization.reader(new DataInputStream(in))
  def writer: ColumnWriter = serialization.writer

  override def toString: String = s"$name -> $serialization"
}
object Column {
  def apply(name: String, serializataion: ColumnSerialization): Column = new Column(name, serializataion)
}

trait ColumnReader {
  def nextRow(): Unit
  def currentValue: Any

  def eq(value: Any): (() => Boolean)
  def in(values: Set[Any]): (() => Boolean)
}

trait ColumnWriter {
  def writeValue(out: DataOutputStream, value: Any): Unit
  def noMoreValues(out: DataOutputStream): Unit = {}

  def writeMetadata(out: DataOutputStream): Unit
}

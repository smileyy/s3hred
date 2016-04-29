package smileyy.s3hred.column

import java.io.{DataInputStream, DataOutputStream, InputStream, OutputStream}

class Column private (val name: String, val serialization: ColumnSerialization) {
  def reader(data: InputStream, meta: InputStream): ColumnReader = {
    serialization.reader(new DataInputStream(data), new DataInputStream(meta))
  }

  def writer(data: OutputStream, meta: OutputStream): ColumnWriter = {
    serialization.writer(new DataOutputStream(data), new DataOutputStream(meta))
  }

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
  def write(value: Any): Unit
  def close(): Unit
}

package smileyy.s3hred.column

import java.io.{InputStream, OutputStream}

class Column(val name: String, serialization: ColumnSerialization) {
  def reader(data: InputStream, meta: InputStream): ColumnReader = {
    serialization.reader(data, meta)
  }

  def writer(data: OutputStream, meta: OutputStream): ColumnWriter = {
    serialization.writer(data, meta)
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

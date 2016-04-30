package smileyy.s3hred.column

import java.io.{InputStream, OutputStream}

/**
  * The serialization format of a [[Column]].
  */
trait ColumnSerialization {
  def reader(data: InputStream, meta: InputStream): ColumnReader
  def writer(data: OutputStream, meta: OutputStream): ColumnWriter

  override def toString: String = getClass.getSimpleName
}

trait ValueSerialization extends ColumnSerialization

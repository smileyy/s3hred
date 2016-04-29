package smileyy.s3hred.column

import java.io.InputStream

/**
  * The representation of a column when serialized.
  */
trait ColumnSerialization {
  def reader(name: String, in: InputStream): ColumnReader
  def writer: ColumnWriter

  override def toString: String = getClass.getSimpleName
}

trait ByteSerialization extends ColumnSerialization
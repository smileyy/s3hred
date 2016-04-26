package smileyy.s3hred.column

import java.io.InputStream

/**
  * The representation of a column when serialized.
  */
trait Representation {
  def reader(name: String, in: InputStream): ColumnReader
  def writer: ColumnWriter
}

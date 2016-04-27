package smileyy.s3hred.column

import java.io.OutputStream

/**
  * Writes column values to an output stream.
  */
trait ColumnWriter {
  def writeValue(out: OutputStream, value: Any): Unit
  def writeMetadata(out: OutputStream): Unit
}

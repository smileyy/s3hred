package smileyy.s3hred.column

import java.io.OutputStream

/**
  * Created by smileyy on 4/15/16.
  */
trait ColumnWriter {
  def writeValue(out: OutputStream, value: Any): Unit
  def writeMetadata(out: OutputStream): Unit
}

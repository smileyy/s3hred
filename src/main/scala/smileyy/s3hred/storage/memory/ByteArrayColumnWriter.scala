package smileyy.s3hred.storage.memory

import java.io.ByteArrayOutputStream

import com.google.common.io.CountingOutputStream
import com.typesafe.scalalogging.LazyLogging
import smileyy.s3hred.column.ColumnWriter

/**
  * Composes an [[ByteArrayOutputStream]] and a [[ColumnWriter]]
  */
private[memory] class ByteArrayColumnWriter(datastream: ByteArrayOutputStream, writer: ColumnWriter)
  extends LazyLogging {

  def write(value: Any): Unit = {
    writer.writeValue(datastream, value)
  }

  def close(): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val counter = new CountingOutputStream(out)

    writer.writeMetadata(counter)
    logger.debug(s"Wrote ${counter.getCount} bytes of metadata")

    val data = datastream.toByteArray
    out.write(datastream.toByteArray)
    logger.debug(s"Wrote ${data.length} bytes of data")

    out.toByteArray
  }
}
private[memory] object ByteArrayColumnWriter {
  def apply(writer: ColumnWriter): ByteArrayColumnWriter = {
    new ByteArrayColumnWriter(new ByteArrayOutputStream(), writer)
  }
}
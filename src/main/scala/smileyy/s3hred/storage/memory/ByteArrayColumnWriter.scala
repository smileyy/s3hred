package smileyy.s3hred.storage.memory

import java.io.{ByteArrayOutputStream, DataOutputStream}

import com.google.common.io.CountingOutputStream
import com.typesafe.scalalogging.LazyLogging
import smileyy.s3hred.column.ColumnWriter

/**
  * Composes an [[ByteArrayOutputStream]] and a [[ColumnWriter]]
  */
private[memory] class ByteArrayColumnWriter(bytestream: ByteArrayOutputStream, writer: ColumnWriter)
  extends LazyLogging {

  val datastream = new DataOutputStream(bytestream)

  def write(value: Any): Unit = {
    writer.writeValue(datastream, value)
  }

  def close(): Array[Byte] = {
    writer.noMoreValues(datastream)

    val out = new ByteArrayOutputStream()
    val counter = new CountingOutputStream(out)

    writer.writeMetadata(new DataOutputStream(counter))
    logger.debug(s"Wrote ${counter.getCount} bytes of metadata")

    val data = bytestream.toByteArray
    out.write(data)
    logger.debug(s"Wrote ${data.length} bytes of data")

    out.toByteArray
  }
}
private[memory] object ByteArrayColumnWriter {
  def apply(writer: ColumnWriter): ByteArrayColumnWriter = {
    new ByteArrayColumnWriter(new ByteArrayOutputStream(), writer)
  }
}
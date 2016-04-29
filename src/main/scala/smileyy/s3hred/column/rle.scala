package smileyy.s3hred.column
import java.io.{DataInputStream, DataOutputStream, InputStream, OutputStream}

import com.typesafe.scalalogging.LazyLogging

/**
  * Run-length-encoding for data in a [[Column]].  RLE can be applied
  * to an underlying serialization format, such as Raw or Tokenized.
  */
class RunLengthEncoding(delegate: ByteSerialization) extends ColumnSerialization {
  override def reader(name: String, in: DataInputStream): ColumnReader = new RleReader(in, delegate.reader(name, in))

  override def writer: ColumnWriter = new RleWriter(delegate.writer)
}
object RunLengthEncoding {
  def apply(serialization: ByteSerialization): ColumnSerialization = new RunLengthEncoding(serialization)
}

class RleWriter(delegate: ColumnWriter) extends ColumnWriter with LazyLogging {
  var last: Option[Any] = None
  var count: Int = 0

  override def writeValue(out: DataOutputStream, newValue: Any): Unit = last match {
    case None =>
      last = Some(newValue)
      count = 1
    case Some(lastValue) =>
      if (newValue == lastValue) {
        count += 1
      }
      else {
        writeCountAndValue(out, lastValue)
        last = Some(newValue)
        count = 1
      }
  }

  override def noMoreValues(out: DataOutputStream): Unit = last match {
    case Some(lastValue) => writeCountAndValue(out, lastValue)
    case None =>
  }

  private def writeCountAndValue(out: DataOutputStream, value: Any): Unit = {
    logger.debug(s"Wrote $count instances of $value")
    out.writeInt(count)
    delegate.writeValue(out, value)
  }

  override def writeMetadata(out: DataOutputStream): Unit = delegate.writeMetadata(out)
}

class RleReader(in: DataInputStream, delegate: ColumnReader) extends ColumnReader with LazyLogging {
  override def name: String = delegate.name

  var counter = 0

  override def nextRow(): Unit = {
    if (counter == 0) {
      counter = in.readInt()
      delegate.nextRow()
      logger.debug(s"Read $counter instances of ${delegate.currentValue}")
    }

    counter -= 1
    logger.debug(s"$counter remaining instances of ${delegate.currentValue}")
  }

  override def currentValue: Any = delegate.currentValue
  override def eq(value: Any): () => Boolean = delegate.eq(value)
  override def in(values: Set[Any]): () => Boolean = delegate.in(values)
}
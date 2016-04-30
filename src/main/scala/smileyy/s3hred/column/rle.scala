package smileyy.s3hred.column
import java.io.{DataInputStream, DataOutputStream, InputStream, OutputStream}

import com.typesafe.scalalogging.LazyLogging

/**
  * Run-length encoding for data in a [[Column]].  RLE can be applied
  * to an underlying serialization format, such as [[Raw]] or [[Tokenized]].
  */
class RunLengthEncoding private (delegate: ValueSerialization) extends ColumnSerialization {
  override def reader(data: InputStream, meta: InputStream): ColumnReader = {
    new RleReader(new DataInputStream(data), delegate.reader(data, meta))
  }
  override def writer(data: OutputStream, meta: OutputStream): ColumnWriter = {
    new RleWriter(new DataOutputStream(data), delegate.writer(data, meta))
  }
}
object RunLengthEncoding {
  def apply(serialization: ValueSerialization): ColumnSerialization = new RunLengthEncoding(serialization)
}

private class RleWriter(data: DataOutputStream, delegate: ColumnWriter) extends ColumnWriter with LazyLogging {
  var last: Option[Any] = None
  var count: Int = 0

  override def write(newValue: Any): Unit = {
    def setNewValue(): Unit = {
      last = Some(newValue)
      count = 1
    }

    last match {
      case None => setNewValue()
      case Some(lastValue) =>
        if (newValue == lastValue) {
          count += 1
        }
        else {
          writeCountAndValue(lastValue)
          setNewValue()
        }
    }
  }

  override def close(): Unit = {
    last match {
      case Some(lastValue) => writeCountAndValue(lastValue)
      case None =>
    }

    delegate.close()
  }

  private def writeCountAndValue(value: Any): Unit = {
    logger.trace(s"Wrote $count instances of $value")
    data.writeInt(count)
    delegate.write(value)
  }
}

private class RleReader(in: DataInputStream, delegate: ColumnReader) extends ColumnReader with LazyLogging {

  var counter = 0

  override def nextRow(): Unit = {
    if (counter == 0) {
      counter = in.readInt()
      delegate.nextRow()
      logger.trace(s"Read $counter instances of ${delegate.currentValue}")
    }

    counter -= 1
    logger.trace(s"$counter remaining instances of ${delegate.currentValue}")
  }

  override def currentValue: Any = delegate.currentValue
  override def eq(value: Any): () => Boolean = delegate.eq(value)
  override def in(values: Set[Any]): () => Boolean = delegate.in(values)
}
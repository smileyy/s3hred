package smileyy.s3hred.storage

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataOutputStream}

import com.google.common.io.CountingOutputStream
import com.typesafe.scalalogging.LazyLogging
import smileyy.s3hred.column.{ColumnReader, ColumnWriter}
import smileyy.s3hred.query.{Select, Where}
import smileyy.s3hred.row.RowIterator
import smileyy.s3hred.{Dataset, RowAddingBuilder}
import smileyy.s3hred.schema.DatasetSchema

/**
  * A [[StorageSystem]] for in-memory datasets
  */
class MemoryStorageSystem extends StorageSystem {
  private var datasets: Map[String, Dataset] = Map.empty

  override def rowAdder(name: String, schema: DatasetSchema): RowAddingBuilder = {
    MemoryStorageRowAdder(this, name, schema)
  }

  private[storage] def createDataset(name: String, storage: MemoryStorage): Dataset = {
    val dataset = new Dataset(name, storage.schema, storage)
    datasets = datasets + (name -> dataset)
    dataset
  }

  override def toString: String = getClass.getSimpleName
}

private class MemoryStorage(val schema: DatasetSchema, data: Map[String, Array[Byte]], numberOfRows: Long)
    extends Storage {

  override def iterator(select: Select, where: Where): Iterator[Seq[Any]] = {
    val readers: Map[String, ColumnReader] = {
      val names = select.columns.toSet ++ where.columns
      names.map { name => name -> schema.column(name).reader(new ByteArrayInputStream(data(name))) }
    }.toMap

    new RowIterator(numberOfRows, readers, select, where)
  }
}

private class MemoryStorageRowAdder(
    mss: MemoryStorageSystem,
    name: String,
    schema: DatasetSchema,
    byteArrayWriters: Seq[ByteArrayColumnWriter])
    extends RowAddingBuilder {

  var rows = 0

  override def add(values: Seq[Any]): RowAddingBuilder = {
    byteArrayWriters.zip(values) foreach { case (writer, value) => writer.write(value) }
    rows += 1
    this
  }

  override def close(): Dataset = {
    val storage = {
      val columns = for (writer <- byteArrayWriters) yield writer.close()
      val columnsByName = schema.columnNames.zip(columns).toMap
      new MemoryStorage(schema, columnsByName, rows)
    }

    mss.createDataset(name, storage)
  }
}
private object MemoryStorageRowAdder {
  def apply(mss: MemoryStorageSystem, name: String, schema: DatasetSchema): MemoryStorageRowAdder = {
    val writers: Seq[ByteArrayColumnWriter] = schema.columns map { column =>
      ByteArrayColumnWriter(column.writer)
    }

    new MemoryStorageRowAdder(mss, name, schema, writers)
  }
}

private class ByteArrayColumnWriter(bytestream: ByteArrayOutputStream, writer: ColumnWriter)
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
private object ByteArrayColumnWriter {
  def apply(writer: ColumnWriter): ByteArrayColumnWriter = {
    new ByteArrayColumnWriter(new ByteArrayOutputStream(), writer)
  }
}
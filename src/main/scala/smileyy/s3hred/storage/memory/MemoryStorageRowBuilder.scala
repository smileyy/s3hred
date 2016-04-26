package smileyy.s3hred.storage.memory

import smileyy.s3hred.Dataset
import smileyy.s3hred.schema.DatasetSchema
import smileyy.s3hred.storage.DatasetRowBuilder

/**
  * Builds in-memory datasets
  */
private[memory] class MemoryStorageRowBuilder(
    mss: MemoryStorageSystem,
    name: String,
    schema: DatasetSchema,
    byteArrayWriters: Seq[ByteArrayColumnWriter])
  extends DatasetRowBuilder {

  var rows = 0

  override def addRowValues(values: Seq[Any]): DatasetRowBuilder = {
    byteArrayWriters.zip(values) map { case (writer, value) => writer.write(value) }
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
private[memory] object MemoryStorageRowBuilder {
  def apply(mss: MemoryStorageSystem, name: String, schema: DatasetSchema): MemoryStorageRowBuilder = {
    val writers: Seq[ByteArrayColumnWriter] = schema.columns map { column =>
      ByteArrayColumnWriter(column.writer)
    }

    new MemoryStorageRowBuilder(mss, name, schema, writers)
  }
}

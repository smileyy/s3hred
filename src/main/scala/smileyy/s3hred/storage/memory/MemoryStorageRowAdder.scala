package smileyy.s3hred.storage.memory

import smileyy.s3hred.{Dataset, RowAddingBuilder}
import smileyy.s3hred.schema.DatasetSchema

/**
  * Builds in-memory datasets
  */
private[memory] class MemoryStorageRowAdder(
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
private[memory] object MemoryStorageRowAdder {
  def apply(mss: MemoryStorageSystem, name: String, schema: DatasetSchema): MemoryStorageRowAdder = {
    val writers: Seq[ByteArrayColumnWriter] = schema.columns map { column =>
      ByteArrayColumnWriter(column.writer)
    }

    new MemoryStorageRowAdder(mss, name, schema, writers)
  }
}

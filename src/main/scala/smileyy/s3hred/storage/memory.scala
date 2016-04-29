package smileyy.s3hred.storage

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import smileyy.s3hred.column.ColumnReader
import smileyy.s3hred.query.{Select, Where}
import smileyy.s3hred.row.RowIterator
import smileyy.s3hred.schema.DatasetSchema
import smileyy.s3hred.{Dataset, RowAddingBuilder}

/**
  * A [[StorageSystem]] for in-memory datasets
  */
class MemoryStorageSystem extends StorageSystem {
  override def rowAdder(schema: DatasetSchema): RowAddingBuilder = {
    new MemoryStorageRowAdder(this, schema)
  }
}

private class MemoryStorage(
    val schema: DatasetSchema,
    val totalNumberOfRows: Long,
    columnArrays: Seq[(Array[Byte], Array[Byte])])
  extends Storage {

  val columns = schema.columnNames.zip(columnArrays).map { case (name, tuple) =>
    name -> tuple
  }.toMap

  override def iterator(select: Select, where: Where): Iterator[Seq[Any]] = {
    val readers: Map[String, ColumnReader] = {
      val names = select.columns.toSet ++ where.columns
      names.map { name =>
        val column = schema.column(name)
        val (data, meta) = columns(name)
        val reader = column.reader(new ByteArrayInputStream(data), new ByteArrayInputStream(meta))
        name -> reader
      }.toMap
    }

    new RowIterator(totalNumberOfRows, readers, select, where)
  }
}

private class MemoryStorageRowAdder(mss: MemoryStorageSystem, schema: DatasetSchema)
  extends RowAddingBuilder {

  var numberOfRows = 0

  val (writers, datastreams, metastreams) = schema.columns.map { column =>
    val data = new ByteArrayOutputStream()
    val meta = new ByteArrayOutputStream()
    val writer = column.writer(data, meta)
    (writer, data, meta)
  }.unzip3

  override def add(values: Seq[Any]): RowAddingBuilder = {
    writers.zip(values) foreach { case (writer, value) => writer.write(value) }
    numberOfRows += 1
    this
  }

  override def close(): Dataset = {
    writers.foreach(_.close())
    datastreams.foreach(_.close())
    metastreams.foreach(_.close())

    val storage = {
      val columnArrays = datastreams.zip(metastreams).map { case (datastream, metastream) =>
        (datastream.toByteArray, metastream.toByteArray)
      }
      new MemoryStorage(schema, numberOfRows, columnArrays)
    }

    new Dataset(schema, storage)
  }
}


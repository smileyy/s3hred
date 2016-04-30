package smileyy.s3hred.storage

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import smileyy.s3hred.column.ColumnReader
import smileyy.s3hred.query.{Select, Where}
import smileyy.s3hred.row.RowIterator
import smileyy.s3hred.schema.DatasetSchema
import smileyy.s3hred.{ByRowBuilderDelegate, ByRowBuilderDelegate$, Dataset, RowAddingBuilder}

/**
  * A [[StorageSystem]] for in-memory datasets
  */
object MemoryStorageSystem extends StorageSystem {
  override def rowAdder(schema: DatasetSchema): RowAddingBuilder = new MemoryStorageRowAdder(schema)
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

private class MemoryStorageRowAdder(schema: DatasetSchema) extends RowAddingBuilder {
  val datastreams = schema.columns.map { c => new ByteArrayOutputStream() }
  val metastreams = schema.columns.map { c => new ByteArrayOutputStream() }

  val delegate = ByRowBuilderDelegate(schema, datastreams, metastreams)

  override def add(row: Seq[Any]): RowAddingBuilder = {
    delegate.add(row)
    this
  }

  override def close(): Dataset = {
    delegate.close()

    val storage = {
      val columnArrays = datastreams.zip(metastreams).map { case (datastream, metastream) =>
        (datastream.toByteArray, metastream.toByteArray)
      }
      new MemoryStorage(schema, delegate.numberOfRows, columnArrays)
    }

    new Dataset(schema, storage)
  }
}


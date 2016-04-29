package smileyy.s3hred.storage.memory

import java.io.ByteArrayInputStream

import smileyy.s3hred.Dataset
import smileyy.s3hred.column.ColumnReader
import smileyy.s3hred.query.{Select, Where}
import smileyy.s3hred.row.RowIterator
import smileyy.s3hred.schema.DatasetSchema
import smileyy.s3hred.storage.Storage

/**
  * In-memory [[Storage]] of a [[Dataset]]
  */
private[memory] class MemoryStorage(val schema: DatasetSchema, data: Map[String, Array[Byte]], numberOfRows: Long)
    extends Storage {

  override def iterator(select: Select, where: Where): Iterator[Seq[Any]] = {
    val readers: Map[String, ColumnReader] = {
      val names = select.columns.toSet ++ where.columns
      names.map { name => name -> schema.column(name).reader(new ByteArrayInputStream(data(name))) }
    }.toMap

    new RowIterator(numberOfRows, readers, select, where)
  }
}

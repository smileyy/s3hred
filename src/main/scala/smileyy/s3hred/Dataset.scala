package smileyy.s3hred

import smileyy.s3hred.column.ColumnReader
import smileyy.s3hred.query._
import smileyy.s3hred.schema.DatasetSchema
import smileyy.s3hred.storage.Storage

/**
  * A read-only column-oriented set of data that can be queried (efficiently?)
  */
class Dataset(schema: DatasetSchema, storage: Storage) {
  def query(select: Option[Select] = None, where: Option[Where] = None): Results = {
    validateColumns(select.map(_.columns).getOrElse(Seq.empty)) { invalid =>
      s"'Select' columns ${invalid.mkString(", ")} not in $schema"
    }
    validateColumns(where.map(_.columns).getOrElse(Set.empty)) { invalid =>
      s"'Where' columns ${invalid.mkString(", ")} not in $schema"
    }

    new DatasetResults(
      select = select.getOrElse(Select(schema.columnNames)),
      where = where.getOrElse(Where(True))
    )
  }

  private def validateColumns(names: Iterable[String])(msg: Seq[String] => String): Unit = {
    val invalid = names.filter { name => !schema.contains(name) }
    if (invalid.nonEmpty) throw new InvalidQueryException(msg(invalid.toSeq))
  }

  class DatasetResults(select: Select, where: Where) extends Results {
    override def columns: Seq[String] = select.columns

    val iterator = storage.iterator(select, where)
    override def hasNext: Boolean = iterator.hasNext
    override def next(): Seq[Any] = iterator.next()
  }

}

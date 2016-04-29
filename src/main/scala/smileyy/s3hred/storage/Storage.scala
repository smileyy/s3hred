package smileyy.s3hred.storage

import smileyy.s3hred.RowAddingBuilder
import smileyy.s3hred.query.{Select, Where}
import smileyy.s3hred.schema.DatasetSchema

/**
  * Stores one or more [[smileyy.s3hred.Dataset]]s and provides constructs
  * for Dataset construction.
  */
trait StorageSystem {
  def rowAdder(name: String, schema: DatasetSchema): RowAddingBuilder
}

trait Storage {
  def iterator(select: Select, where: Where): Iterator[Seq[Any]]
}

/**
  * A null-object [[Storage]] implementation, primarily used for testing.
  */
object NullStorage extends Storage {
  override def iterator(select: Select, where: Where): Iterator[Seq[Any]] = Iterator.empty
}

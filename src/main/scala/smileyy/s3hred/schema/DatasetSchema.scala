package smileyy.s3hred.schema

import smileyy.s3hred.{Dataset, RowAddingBuilder}
import smileyy.s3hred.column.tokenized.Tokenized
import smileyy.s3hred.column.Column
import smileyy.s3hred.storage.StorageSystem

/**
  * Defines the schema of a [[smileyy.s3hred.Dataset]]
  *
  * Presently only [[Tokenized]] string-valued columns are supported
  */
class DatasetSchema(val columns: Seq[Column]) {
  val columnNames: Seq[String] = columns.map(_.name)

  private val columnsByName: Map[String, Column] = {
    val map = columns.map({ c => c.name -> c }).toMap
    if (map.keySet.size < columns.size) throw new InvalidSchemaException(s"Column names must be unique: $columnNames")
    map
  }

  def contains(name: String): Boolean = columnsByName.keySet.contains(name)
  def column(name: String): Column = columnsByName(name)
  def column(idx: Int): Column = columns(idx)

  def newDatasetByRows(name: String, storage: StorageSystem)(f: RowAddingBuilder => Unit): Dataset = {
    val adder = storage.rowAdder(name, this)
    f(adder)
    adder.close()
  }
}
object DatasetSchema {
  def apply(names: String*): DatasetSchema = {
    new DatasetSchema(names.map { name => new Column(name, Tokenized()) })
  }
}
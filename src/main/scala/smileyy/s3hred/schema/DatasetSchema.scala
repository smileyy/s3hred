package smileyy.s3hred.schema

import smileyy.s3hred.column.raw.Raw
import smileyy.s3hred.{Dataset, RowAddingBuilder}
import smileyy.s3hred.column.tokenized.Tokenized
import smileyy.s3hred.column.{Column, Representation}
import smileyy.s3hred.storage.StorageSystem

/**
  * Defines the schema of a [[smileyy.s3hred.Dataset]]
  *
  * Presently only [[Tokenized]] string-valued columns are supported
  */
class DatasetSchema(val name: String, val columns: Seq[Column]) {
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

  override def toString: String = s"${getClass.getSimpleName}${columns.mkString("[", ", ", "]")}"
}
object DatasetSchema {
  def apply(name: String, columns: Seq[(String, Representation)]): DatasetSchema = {
    new DatasetSchema(name, columns.map { case (n, repr) => Column(n, repr) })
  }

  def rawSchema(name: String, columns: Seq[String]): DatasetSchema = {
    new DatasetSchema(name, columns.map(Column(_, Raw)))
  }
}
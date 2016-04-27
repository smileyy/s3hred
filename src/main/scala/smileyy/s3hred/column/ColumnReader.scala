package smileyy.s3hred.column

/**
  * Reads a [[Column]] of data in a [[smileyy.s3hred.Dataset]]
  */
trait ColumnReader {
  def name: String
  def nextRow(): Unit
  def currentValue: Any

  def eq(value: Any): (() => Boolean)
  def in(values: Set[Any]): (() => Boolean)
}
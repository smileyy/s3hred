package smileyy.s3hred.column

import smileyy.s3hred.row.RowFilter

/**
  * Reads a [[Column]] of data in a [[smileyy.s3hred.Dataset]]
  */
trait ColumnReader {
  def name: String
  def nextRow(): Unit
  def currentValue: Any

  def eq(value: Any): RowFilter
  def in(values: Set[Any]): RowFilter
}
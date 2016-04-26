package smileyy.s3hred.storage

import smileyy.s3hred.Dataset

/**
  * Create a [[Dataset]], row by row
  */
trait DatasetRowBuilder {
  def addRow(values: Any*): DatasetRowBuilder = addRowValues(values.toSeq)

  // TODO: could we use something like shapeless to make this typesafe?
  def addRowValues(values: Seq[Any]): DatasetRowBuilder

  def close(): Dataset
}



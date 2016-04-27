package smileyy.s3hred.storage

import smileyy.s3hred.RowAddingBuilder
import smileyy.s3hred.schema.DatasetSchema

/**
  * A [[Storage]] system for [[smileyy.s3hred.Dataset]]s
  */
trait StorageSystem {
  def rowAdder(name: String, schema: DatasetSchema): RowAddingBuilder
}

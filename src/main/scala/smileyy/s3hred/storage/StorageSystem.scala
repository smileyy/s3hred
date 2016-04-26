package smileyy.s3hred.storage

import smileyy.s3hred.schema.DatasetSchema

/**
  * A [[Storage]] system for [[smileyy.s3hred.Dataset]]s
  */
trait StorageSystem {
  def datasetRowBuilder(name: String, schema: DatasetSchema): DatasetRowBuilder
}

package smileyy.s3hred.storage.memory

import smileyy.s3hred.{Dataset, RowAddingBuilder}
import smileyy.s3hred.schema.DatasetSchema
import smileyy.s3hred.storage.StorageSystem

/**
  * A [[StorageSystem]] for in-memory datasets
  */
class MemoryStorageSystem extends StorageSystem {
  private var datasets: Map[String, Dataset] = Map.empty

  override def rowAdder(name: String, schema: DatasetSchema): RowAddingBuilder = {
    MemoryStorageRowAdder(this, name, schema)
  }

  private[memory] def createDataset(name: String, storage: MemoryStorage): Dataset = {
    val dataset = new Dataset(name, storage.schema, storage)
    datasets = datasets + (name -> dataset)
    dataset
  }

  override def toString: String = getClass.getSimpleName
}

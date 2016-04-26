package smileyy.s3hred.storage.memory

import smileyy.s3hred.Dataset
import smileyy.s3hred.schema.DatasetSchema
import smileyy.s3hred.storage.{DatasetRowBuilder, StorageSystem}

/**
  * A [[StorageSystem]] for in-memory datasets
  */
class MemoryStorageSystem extends StorageSystem {
  private var datasets: Map[String, Dataset] = Map.empty

  override def datasetRowBuilder(name: String, schema: DatasetSchema): DatasetRowBuilder = {
    MemoryStorageRowBuilder(this, name, schema)
  }

  private[memory] def createDataset(name: String, storage: MemoryStorage): Dataset = {
    val dataset = new Dataset(name, storage.schema, storage)
    datasets = datasets + (name -> dataset)
    dataset
  }
}

package smileyy.s3hred.storage

import org.scalatest.FlatSpec
import smileyy.s3hred.DatasetBehaviors
import smileyy.s3hred.ItemTestData

/**
  * Trait for testing [[StorageSystem]] implementations.
  */
trait StorageBehaviors extends DatasetBehaviors { this: FlatSpec =>
  import ItemTestData._

  def newStorageSystem: StorageSystem

  Schemas.foreach { case (name, schema) =>
    val storage = newStorageSystem

    val dataset = schema.newDatasetByRows(storage) { rowAdder =>
      for (row <- ItemData) { rowAdder.add(row) }
    }

    s"$name stored in $storage" should behave like aDataset(dataset)
  }
}

package smileyy.s3hred.storage.memory

import org.scalatest.{FlatSpec, Matchers}
import smileyy.s3hred.{DatasetBehaviors, ItemTestData}
import smileyy.s3hred.query.QueryDSL
import smileyy.s3hred.schema.DatasetSchema
import smileyy.s3hred.storage.StorageSystem
import smileyy.s3hred.util.Table

/**
  * Tests [[MemoryStorageSystem]] and [[MemoryStorage]]
  */
class MemoryStorageSpec extends FlatSpec with DatasetBehaviors {
  def newStorageSystem = new MemoryStorageSystem
}

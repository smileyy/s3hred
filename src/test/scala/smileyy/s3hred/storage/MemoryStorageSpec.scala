package smileyy.s3hred.storage

import org.scalatest.FlatSpec
import smileyy.s3hred.DatasetBehaviors

/**
  * Tests [[MemoryStorageSystem]] and [[MemoryStorage]]
  */
class MemoryStorageSpec extends FlatSpec with DatasetBehaviors {
  def newStorageSystem = new MemoryStorageSystem
}

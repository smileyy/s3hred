package smileyy.s3hred.storage

import java.nio.file.{Files, Paths}
import java.util.UUID

import org.scalatest.FlatSpec
import smileyy.s3hred.DatasetBehaviors

/**
  * Tests [[FileStorage]] and [[FileStorageSystem]]
  */
class FileStorageSpec extends FlatSpec with StorageBehaviors {
  override def newStorageSystem: StorageSystem = {
    val storageSystemDir = {
      val path = Paths.get(s"target/FileStorageSpec-${UUID.randomUUID.toString}")
      Files.deleteIfExists(path)
      Files.createDirectories(path)
      path
    }

    new FileStorageSystem(storageSystemDir)
  }
}

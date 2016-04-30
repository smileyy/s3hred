package smileyy.s3hred.csv

import java.io.File

import org.scalatest.FlatSpec
import smileyy.s3hred.DatasetBehaviors
import smileyy.s3hred.column.{Column, Raw}
import smileyy.s3hred.storage.MemoryStorageSystem

/**
  * Created by smileyy on 4/29/16.
  */
class CsvSpec extends FlatSpec with DatasetBehaviors {
  val csv = new File("src/test/resources/csv/items.csv")
  val mapping = (name: String) => new Column(name, Raw)

  val dataset = new CsvDatasetBuilder(MemoryStorageSystem, mapping).fromFile(csv)

  s"A dataset build from CSV" should behave like aDataset(dataset)
}

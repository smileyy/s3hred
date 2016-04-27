package smileyy.s3hred

import smileyy.s3hred.column.raw.Raw
import smileyy.s3hred.column.tokenized.Tokenized
import smileyy.s3hred.schema.DatasetSchema
import smileyy.s3hred.util.Table

/**
  * Provides some test data and schema related to some notion of 'items'.
  */
object ItemTestData {

  val DatasetName = "Test Items"

  val ItemColumnNames = Seq("Size", "Color", "Shape")

  val Schemas = List(
    DatasetSchema("Raw", ItemColumnNames.map(_ -> Raw)),
    DatasetSchema("Tokenized", ItemColumnNames.map(_ -> Tokenized()))
  )

  val ItemData = Seq(
      Seq("Large", "Blue", "Box"),
      Seq("Small", "Blue", "Circle"),
      Seq("Small", "Purple", "Circle"),
      Seq("Large", "Purple", "Circle"),
      Seq("Large", "Purple", "Box"),
      Seq("Medium", "Pink", "Triangle")
  )

  val ItemsDataTable = Table(ItemColumnNames, ItemData)
}

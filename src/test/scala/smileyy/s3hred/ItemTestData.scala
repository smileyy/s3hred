package smileyy.s3hred

import smileyy.s3hred.schema.DatasetSchema
import smileyy.s3hred.util.Table

/**
  * Provides some test data and schema related to some notion of 'items'.
  */
object ItemTestData {

  val ItemsName = "Items"
  val ItemsSchema = DatasetSchema("Size", "Color", "Shape")
  val ItemsData = Seq(
      Seq("Large", "Blue", "Box"),
      Seq("Small", "Blue", "Circle"),
      Seq("Small", "Purple", "Circle"),
      Seq("Large", "Purple", "Circle"),
      Seq("Large", "Purple", "Box"),
      Seq("Medium", "Pink", "Triangle")
  )

  val ItemsDataTable = Table(ItemsSchema.columnNames, ItemsData)
}

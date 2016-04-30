package smileyy.s3hred

import smileyy.s3hred.column.{Raw, RunLengthEncoding, RunLengthEncoding$, Tokenized}
import smileyy.s3hred.schema.DatasetSchema
import smileyy.s3hred.util.Table

/**
  * Provides some test data and schema related to some notion of 'items'.
  */
object ItemTestData {

  val ItemColumnNames = Seq("Size", "Color", "Shape")

  val Schemas = List(
    "Raw" -> DatasetSchema(ItemColumnNames.map(_ -> Raw)),
    "Tokenized" -> DatasetSchema(ItemColumnNames.map(_ -> Tokenized())),
    "RunLengthEncodedRaw" -> DatasetSchema(ItemColumnNames.map(_ -> RunLengthEncoding(Raw))),
    "RunLengthEncodedTokenized" -> DatasetSchema(ItemColumnNames.map(_ -> RunLengthEncoding(Tokenized())))
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

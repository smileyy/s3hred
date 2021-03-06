package smileyy.s3hred

import org.scalatest.{FlatSpec, Matchers}
import smileyy.s3hred.query.{InvalidQueryException, QueryDSL}
import smileyy.s3hred.schema.DatasetSchema
import smileyy.s3hred.storage.NullStorage

/**
  * Tests the basics of a [[Dataset]] regardless of schema or storage
  */
class DatasetSpec extends FlatSpec with Matchers {
  import DatasetSchema._
  import QueryDSL._

  val schema = rawSchema("FooSchema", Seq("Foo"))
  val dataset = new Dataset(schema, NullStorage)

  "A dataset" should "reject queries selecting undefined columns" in {
    intercept[InvalidQueryException] {
      dataset.query(
        select("Foo", "Bar")
      )
    }
  }
}

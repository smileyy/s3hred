package smileyy.s3hred.schema

import org.scalatest.{FlatSpec, Matchers}

/**
  * Tests [[DatasetSchema]] creation.
  */
class DatasetSchemaSpec extends FlatSpec with Matchers {
  import DatasetSchema._

  "A schema" should "not allow duplicate column names" in {
    an [InvalidSchemaException] should be thrownBy rawSchema("InvalidSchema", Seq("Foo", "Bar", "Foo"))
  }
}
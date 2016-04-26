package smileyy.s3hred.schema

import org.scalatest.{FlatSpec, Matchers}

/**
  * Tests [[DatasetSchema]] creation.
  */
class DatasetSchemaSpec extends FlatSpec with Matchers {
  "A schema" should "not allow duplicate column names" in {
    intercept[InvalidSchemaException] {
      DatasetSchema("Foo", "Bar", "Foo")
    }
  }
}
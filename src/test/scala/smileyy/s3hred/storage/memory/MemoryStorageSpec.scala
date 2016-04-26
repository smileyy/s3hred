package smileyy.s3hred.storage.memory

import org.scalatest.{FlatSpec, Matchers}
import smileyy.s3hred.query.QueryDSL
import smileyy.s3hred.schema.DatasetSchema

/**
  * Tests [[MemoryStorageSystem]] and [[MemoryStorage]]
  */
class MemoryStorageSpec extends FlatSpec with Matchers {

  import QueryDSL._

  val storage = new MemoryStorageSystem()
  val schema = DatasetSchema("Size", "Color", "Shape")

  val dataset = {
    storage.datasetRowBuilder("SizesShapesColors", schema)
        .addRow("Large", "Blue", "Box")
        .addRow("Small", "Blue", "Circle")
        .addRow("Small", "Purple", "Circle")
        .addRow("Large", "Purple", "Circle")
        .addRow("Medium", "Pink", "Triangle")
        .close()
  }

  "An in-memory dataset" should "return all columns" in {
    val results = dataset.query(select("*"))
    val rows = results.toSeq
    rows.size shouldBe 5
    rows.head shouldBe Seq("Large", "Blue", "Box")
    rows(4) shouldBe Seq("Medium", "Pink", "Triangle")
  }

  it should "return selected columns" in {
    val results = dataset.query(select("Size", "Shape"))
    val rows = results.toSeq
    rows.size shouldBe 5
    rows.head shouldBe Seq("Large", "Box")
    rows(4) shouldBe Seq("Medium", "Triangle")
  }

  it should "return selected columns in any order" in {
    val results = dataset.query(select("Shape", "Color"))
    val rows = results.toSeq
    rows.size shouldBe 5
    rows.head shouldBe Seq("Box", "Blue")
    rows(4) shouldBe Seq("Triangle", "Pink")
  }

  it should "return duplicate columns" in {
    val results = dataset.query(select("Shape", "Shape"))
    val rows = results.toSeq
    rows.size shouldBe 5
    rows.head shouldBe Seq("Box", "Box")
    rows(4) shouldBe Seq("Triangle", "Triangle")
  }

  it should "support ~=~ expressions that match zero rows" in {
    val results = dataset.query(select("*"), where("Size" ~=~ "Humongous"))
    val rows = results.toSeq
    rows.size shouldBe 0
  }

  it should "support ~=~ expressions that match some rows " in {
    val results = dataset.query(select("*"), where("Size" ~=~ "Large"))
    val rows = results.toSeq
    rows.size shouldBe 2
    rows.head shouldBe Seq("Large", "Blue", "Box")
    rows(1) shouldBe Seq("Large", "Purple", "Circle")
  }

  it should "support && expressions" in {
    val results = dataset.query(select("*"), where(("Size" ~=~ "Large") && ("Color" ~=~ "Blue")))
    val rows = results.toSeq
    rows.size shouldBe 1
    rows.head shouldBe Seq("Large", "Blue", "Box")
  }

  it should "support chained && expressions" in {
    val results = dataset.query(
      select("*"),
      where(("Size" ~=~ "Large") && ("Color" ~=~ "Blue") && ("Shape" ~=~ "Box"))
    )

    val rows = results.toSeq
    rows.size shouldBe 1
    rows.head shouldBe Seq("Large", "Blue", "Box")
  }

  it should "support || expressions" in {
    val results = dataset.query(select("*"), where(("Size" ~=~ "Small") || ("Color" ~=~ "Blue")))
    val rows = results.toSeq
    rows.size shouldBe 3
    rows.head shouldBe Seq("Large", "Blue", "Box")
    rows(1) shouldBe Seq("Small", "Blue", "Circle")
    rows(2) shouldBe Seq("Small", "Purple", "Circle")
  }

  it should "support && and || expressions" in {
    val results = dataset.query(
      select("*"),
      where((("Color" ~=~ "Purple") || ("Size" ~=~ "Large")) && ("Shape" ~=~ "Box"))
    )
    val rows = results.toSeq
    rows.size shouldBe 1
    rows shouldBe Seq(
      Seq("Large", "Blue", "Box")
    )
  }

  it should "support the 'in' operator" in {
    val results = dataset.query(
      select("*"),
      where("Color" in ("Purple", "Pink"))
    )
    val rows = results.toSeq
    rows.size shouldBe 3
    rows shouldBe Seq(
      Seq("Small", "Purple", "Circle"),
      Seq("Large", "Purple", "Circle"),
      Seq("Medium", "Pink", "Triangle")
    )
  }
}

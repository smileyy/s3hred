package smileyy.s3hred.storage.memory

import org.scalatest.{FlatSpec, Matchers}
import smileyy.s3hred.ItemTestData
import smileyy.s3hred.query.QueryDSL
import smileyy.s3hred.schema.DatasetSchema
import smileyy.s3hred.util.Table

/**
  * Tests [[MemoryStorageSystem]] and [[MemoryStorage]]
  */
class MemoryStorageSpec extends FlatSpec with Matchers {

  import QueryDSL._
  import ItemTestData._

  val storage = new MemoryStorageSystem()

  val dataset = ItemsSchema.newDatasetByRows(ItemsName, storage) { rowAdder =>
    for (row <- ItemsData) { rowAdder.add(row) }
  }

  "An in-memory dataset" should "return all columns" in {
    dataset.query(select("*")).toTable shouldBe ItemsDataTable
  }

  it should "return selected columns" in {
    dataset.query(select("Size", "Shape")).toTable shouldBe ItemsDataTable.withColumns(Seq("Size","Shape"))
  }

  it should "return selected columns in any order" in {
    dataset.query(select("Shape", "Color")).toTable shouldBe ItemsDataTable.withColumns(Seq("Shape", "Color"))
  }

  it should "return duplicate columns" in {
    dataset.query(select("Shape", "Shape")).toTable shouldBe ItemsDataTable.withColumns(Seq("Shape", "Shape"))
  }

  it should "support ~=~ expressions that match zero rows" in {
    dataset.query(select("*"), where("Size" ~=~ "Humongous")).toTable shouldBe ItemsDataTable.empty
  }

  it should "support ~=~ expressions that match some rows " in {
    dataset.query(select("*"), where("Size" ~=~ "Large")).toTable shouldBe ItemsDataTable.rows(0, 3, 4)
  }

  it should "support && expressions" in {
    dataset.query(
      select("*"),
      where(("Size" ~=~ "Large") && ("Color" ~=~ "Blue"))
    ).toTable shouldBe ItemsDataTable.rows(0)
  }

  it should "support chained && expressions" in {
    dataset.query(
      select("*"),
      where(("Size" ~=~ "Large") && ("Color" ~=~ "Blue") && ("Shape" ~=~ "Box"))
    ).toTable shouldBe ItemsDataTable.rows(0)
  }

  it should "support || expressions" in {
    dataset.query(
      select("*"),
      where(("Size" ~=~ "Small") || ("Color" ~=~ "Blue"))
    ).toTable shouldBe ItemsDataTable.rows(0, 1, 2)
  }

  it should "support && and || expressions" in {
    dataset.query(
      select("*"),
      where((("Color" ~=~ "Purple") || ("Size" ~=~ "Large")) && ("Shape" ~=~ "Box"))
    ).toTable shouldBe ItemsDataTable.rows(0, 4)
  }

  it should "support the 'in' operator" in {
    dataset.query(
      select("*"),
      where("Color" in ("Purple", "Pink"))
    ).toTable shouldBe ItemsDataTable.rows(2, 3, 4, 5)
  }
}

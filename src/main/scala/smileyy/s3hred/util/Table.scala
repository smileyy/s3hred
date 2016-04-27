package smileyy.s3hred.util

/**
  * An in-memory m x n table with column names, primarily used for string
  * formatting and other messaging re: data
  */
case class Table(columns: Seq[String], data: Seq[Seq[Any]]) {
  data.zipWithIndex.foreach { case (row, idx) =>
    assert(row.size == columns.size, s"Row $idx has ${row.size} columns, should have ${columns.size}")
  }

  private val indexes: Map[String, Int] = columns.zipWithIndex.toMap

  /**
    * Returns a table containing only the rows corresponding to the given indexes
    */
  def rows(idxs: Int*): Table = Table(columns, idxs.map { idx => data(idx) })

  /**
    * Returns a Table with only the given columns
    */
  def withColumns(newColumns: Seq[String]): Table = {
    newColumns.foreach { col => assert(columns.contains(col)) }

    val newRows = data.map { row => newColumns.map(indexes).map { idx => row(idx) } }
    Table(newColumns, newRows)
  }

  /**
    * Returns a table with the same columns and no data
    */
  def empty: Table = Table(columns, Seq.empty)



  override def toString: String = {
    val cols = columns.mkString("[", ", ", "]")
    val rows = data.map(_.mkString("(", ",", ")")).mkString("[", ", ", "]")
    s"Table with ${columns.size} columns and ${data.size} rows: $cols$rows"
  }
}

package smileyy.s3hred.row

import smileyy.s3hred.column.ColumnReader
import smileyy.s3hred.query._

import scala.annotation.tailrec

/**
  * Iterates over a sequence of [[ColumnReader]]s to produce a row.
  */
private[s3hred] class RowIterator(rows: Long, readers: Set[ColumnReader], select: Select, where: Where)
    extends Iterator[Seq[Any]] {

  val readersByName: Map[String, ColumnReader] = readers.map { r => r.name -> r }.toMap
  val selected: Seq[ColumnReader] = select.columns.map(readersByName)
  val filter = RowFilter(readersByName, where.expr)

  var rowsRead: Long = 0
  var nextRow: Option[Seq[Any]] = readNextRow()

  @tailrec
  private def readNextRow(): Option[Seq[Any]] = {
    def advance(): Unit = {
      readers.foreach { r => r.nextRow()}
      rowsRead += 1
    }

    if (rowsRead == rows) None
    else {
      advance()
      if (filter.acceptsRow()) Some(selected.map(_.currentValue))
      else readNextRow()
    }
  }

  override def hasNext: Boolean = nextRow.isDefined
  override def next(): Seq[Any] = {
    val values = nextRow.get
    nextRow = readNextRow()
    values
  }
}
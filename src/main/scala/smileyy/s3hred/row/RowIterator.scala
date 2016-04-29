package smileyy.s3hred.row

import com.typesafe.scalalogging.LazyLogging
import smileyy.s3hred.column.ColumnReader
import smileyy.s3hred.query._

import scala.annotation.tailrec

/**
  * Iterates over a sequence of [[ColumnReader]]s to produce a row.
  */
private[s3hred] class RowIterator(rows: Long, readers: Map[String, ColumnReader], select: Select, where: Where)
    extends Iterator[Seq[Any]] with LazyLogging {

  val selected: Seq[ColumnReader] = select.columns.map(readers)
  val predicate: () => Boolean = createPredicate(where.expr)

  var rowsRead: Long = 0
  var nextRow: Option[Seq[Any]] = readNextRow()

  @tailrec
  private def readNextRow(): Option[Seq[Any]] = {
    def advance(): Unit = {
      readers.values.foreach { r => r.nextRow()}
      rowsRead += 1
    }

    if (rowsRead == rows) None
    else {
      advance()
      if (predicate()) Some(selected.map(_.currentValue))
      else readNextRow()
    }
  }

  override def hasNext: Boolean = nextRow.isDefined
  override def next(): Seq[Any] = {
    val values = nextRow.get
    nextRow = readNextRow()
    values
  }

  private def createPredicate(expr: WhereExpr): () => Boolean = {
    logger.debug(s"Creating predicates for $expr")

    def and(predicates: List[() => Boolean]): () => Boolean = () => predicates.forall { p => p() }
    def or(predicates: List[() => Boolean]): () => Boolean = () => predicates.exists { p => p() }

    expr match {
      case Equals(column, value) => readers(column).eq(value)
      case In(column, values) => readers(column).in(values)
      case And(exprs) => and(exprs.map { e => createPredicate(e) })
      case Or(exprs) => or(exprs.map(createPredicate))
      case True => () => true
    }
  }
}
package smileyy.s3hred.row

import smileyy.s3hred.column.ColumnReader
import smileyy.s3hred.query._

trait RowFilter {
  def acceptsRow(): Boolean
}
object RowFilter {
  def apply(readers: Map[String, ColumnReader], expr: WhereExpr): RowFilter = {
    expr match {
      case Equals(column, value) => readers(column).eq(value)
      case In(column, values) => readers(column).in(values)
      case And(exprs) => AndFilter(readers, exprs)
      case Or(exprs) => OrFilter(readers, exprs)
      case True => TrueFilter
    }
  }

  private class AndFilter(filters: Iterable[RowFilter]) extends RowFilter {
    override def acceptsRow(): Boolean = {
      filters.forall(_.acceptsRow())
    }
  }
  private object AndFilter {
    def apply(readers: Map[String, ColumnReader], exprs: List[WhereExpr]): AndFilter = {
      new AndFilter(exprs.map(RowFilter(readers, _)))
    }
  }

  private class OrFilter(filters: Iterable[RowFilter]) extends RowFilter {
    override def acceptsRow(): Boolean = {
      filters.exists(_.acceptsRow())
    }
  }
  private object OrFilter {
    def apply(readers: Map[String, ColumnReader], exprs: List[WhereExpr]): OrFilter = {
      new OrFilter(exprs.map(RowFilter(readers, _)))
    }
  }
}






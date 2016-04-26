package smileyy.s3hred.query

/**
  * A DSL for specifying query parameters.
  *
  * TODO: Provide examples
  */
object QueryDSL {
  def select(column: String, columns: String*): Option[Select] = {
    if (columns.isEmpty && column == "*") None
    else Some(Select(column +: columns.toSeq))
  }

  def where(expr: WhereExpr): Option[Where] = Some(Where(expr))

  implicit class ColumnExpression(column: String) {
    def ~=~(value: Any) = Equals(column, value)
    def in(values: Any*) = In(column, values.toSet)
  }
}

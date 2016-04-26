package smileyy.s3hred.query

/**
  * The "select" clause of a query
  */
case class Select(columns: Seq[String])

case class Where(expr: WhereExpr) {
  def columns: Set[String] = expr.columns
}

sealed trait WhereExpr {
  def columns: Set[String]

  def and(expr: WhereExpr): WhereExpr = And(this, expr)
  def &&(expr: WhereExpr): WhereExpr = And(this, expr)

  def or(expr: WhereExpr): WhereExpr = Or(this, expr)
  def ||(expr: WhereExpr): WhereExpr = Or(this, expr)
}

case class Equals(column: String, value: Any) extends WhereExpr {
  override def columns: Set[String] = Set(column)
}

case class In(column: String, values: Set[Any]) extends WhereExpr {
  override def columns: Set[String] = Set(column)
}

case class And(exprs: List[WhereExpr]) extends WhereExpr {
  override def columns: Set[String] = exprs.map(_.columns).reduceLeft(_++_)
}
object And {
  def apply(l: WhereExpr, r: WhereExpr): And = (l, r) match {
    case (la: And, _) => And(r +: la.exprs)
    case (_, ra: And) => And(l +: ra.exprs)
    case (_,_) => And(List(l,r))
  }
}

case class Or(exprs: List[WhereExpr]) extends WhereExpr {
  override def columns: Set[String] = exprs.map(_.columns).reduceLeft(_++_)
}
object Or {
  def apply(l: WhereExpr, r: WhereExpr): Or = (l, r) match {
    case (lo: And, _) => Or(r +: lo.exprs)
    case (_, ro: Or) => Or(l +: ro.exprs)
    case (_,_) => Or(List(l,r))
  }
}

case object True extends WhereExpr {
  override def columns: Set[String] = Set.empty
}
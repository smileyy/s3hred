package smileyy.s3hred.row

object TrueFilter extends RowFilter {
  override def acceptsRow(): Boolean = true
}

object FalseFilter extends RowFilter {
  override def acceptsRow(): Boolean = false
}
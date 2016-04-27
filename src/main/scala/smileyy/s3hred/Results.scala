package smileyy.s3hred

import smileyy.s3hred.util.Table

trait Results extends Iterator[Seq[Any]] {
  def columns: Seq[String]
  def toTable: Table = Table(columns, this.toSeq)
}
package smileyy.s3hred.column.tokenized

import java.io.InputStream

import smileyy.s3hred.column.{ColumnReader, ColumnWriter, Representation}

/**
  * A column that a symbol table and tokens to represent values.
  */
class Tokenized extends Representation {
  override def reader(name: String, in: InputStream): ColumnReader = TokenizedColumnReader(name, in)
  override def writer: ColumnWriter = new TokenizedColumnWriter()
}
object Tokenized {
  def apply(): Tokenized = new Tokenized()
}
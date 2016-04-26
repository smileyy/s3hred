package smileyy.s3hred.column.tokenized

import java.io.{DataInputStream, InputStream}

import smileyy.s3hred.column.ColumnReader
import smileyy.s3hred.row.{FalseFilter, RowFilter}
import smileyy.s3hred.util.io.EnhancedStreams

/**
  * Reads the values from a column with [[Tokenized]] representation
  */
private[tokenized] class TokenizedColumnReader(val name: String, tokens: SymbolTable, in: DataInputStream)
    extends ColumnReader {

  var currentToken = -1

  override def nextRow(): Unit = {
    currentToken = in.readInt()
  }

  override def currentValue: Any = tokens(currentToken)

  override def eq(value: Any): RowFilter = {
    tokens.getToken(value).map(new EqFilter(_)).getOrElse(FalseFilter)
  }

  override def in(values: Set[Any]): RowFilter = {
    val matchingTokens = values.flatMap(tokens.getToken)
    if (matchingTokens.isEmpty) FalseFilter else new InFilter(matchingTokens)
  }

  class EqFilter(token: Int) extends RowFilter {
    override def acceptsRow(): Boolean = token == currentToken
  }

  class InFilter(tokens: Set[Int]) extends RowFilter {
    override def acceptsRow(): Boolean = tokens.contains(currentToken)
  }
}
private[tokenized] object TokenizedColumnReader {
  def apply(name: String, in: InputStream): TokenizedColumnReader = {
    val datastream = new DataInputStream(in)
    val tokens = SymbolTable.deserialize(datastream)
    new TokenizedColumnReader(name, tokens, datastream)
  }
}
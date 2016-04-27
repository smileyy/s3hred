package smileyy.s3hred.column.tokenized

import java.io.{DataInputStream, InputStream}

import smileyy.s3hred.column.ColumnReader

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

  override def eq(value: Any): () => Boolean = {
    tokens.getToken(value) match {
      case Some(token) => () => token == currentToken
      case None => () => false
    }
  }

  override def in(values: Set[Any]): () => Boolean = {
    def predicate(tokens: Set[Int]): Boolean = {
      tokens.contains(currentToken)
    }

    val matchingTokens = values.flatMap(tokens.getToken)
    if (matchingTokens.isEmpty) () => false
    else () => predicate(matchingTokens)
  }
}
private[tokenized] object TokenizedColumnReader {
  def apply(name: String, in: InputStream): TokenizedColumnReader = {
    val datastream = new DataInputStream(in)
    val tokens = SymbolTable.deserialize(datastream)
    new TokenizedColumnReader(name, tokens, datastream)
  }
}
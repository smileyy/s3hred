package smileyy.s3hred.column

import java.io.{DataInputStream, DataOutputStream, InputStream, OutputStream}

import com.google.common.io.CountingOutputStream
import com.typesafe.scalalogging.LazyLogging
import smileyy.s3hred.util.io.{ByteSerializers, EnhancedStreams}

/**
  * A column that a symbol table and tokens to represent values.
  */
class Tokenized extends ValueSerialization {
  override def reader(data: InputStream, meta: InputStream): ColumnReader = {
    new TokenizedColumnReader(new DataInputStream(data), new DataInputStream(meta))
  }
  override def writer(data: OutputStream, meta: OutputStream): ColumnWriter = {
    new TokenizedColumnWriter(new DataOutputStream(data), new DataOutputStream(meta))
  }
}
object Tokenized {
  def apply(): Tokenized = new Tokenized()
}

private class TokenizedColumnReader(data: DataInputStream, meta: DataInputStream)
    extends ColumnReader {

  val tokens = SymbolTable.deserialize(meta)
  var currentToken = -1

  override def nextRow(): Unit = {
    currentToken = data.readInt()
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

private class TokenizedColumnWriter(data: DataOutputStream, meta: DataOutputStream)
    extends ColumnWriter with LazyLogging {

  var tokens = SymbolTable.empty

  override def write(value: Any): Unit = {
    tokens = tokens.tokenizeAnd(value) { token => data.writeInt(token) }
  }

  override def close(): Unit = {
    tokens.serializeTo(meta)
  }
}

/**
  * A two-way mapping from token (Byte|Short|Int) <-> value (Any)
  *
  * TODO: consider uniform types?
  * TODO: consider allowing shorter values
  */
class SymbolTable private (tokenToValue: Map[Int, Any], valueToToken: Map[Any, Int], size: Int)
    extends LazyLogging {

  def apply(token: Int): Any = tokenToValue(token)

  def getToken(value: Any): Option[Int] = valueToToken.get(value)

  def tokenize(value: Any): (Int, SymbolTable) = {
    valueToToken.get(value) match {
      case Some(token) =>
        (token, this)
      case None =>
        val newToken = size
        val newTable = new SymbolTable(
          tokenToValue + (newToken -> value),
          valueToToken + (value -> newToken),
          size + 1
        )
        (newToken, newTable)
    }
  }

  def tokenizeAnd(value: Any)(f: Int => Unit): SymbolTable = {
    val (token, table) = tokenize(value)
    f(token)
    table
  }

  def serializeTo(out: OutputStream): Long = {
    import ByteSerializers._
    import EnhancedStreams._

    val counter = new CountingOutputStream(out)
    counter.writeInt(size)

    tokenToValue.foldLeft(counter) { case (stream, (token, value)) =>
      stream.writeInt(token).writeLengthValue(serializeValue(value))
    }

    counter.getCount
  }
}
object SymbolTable extends LazyLogging {
  val empty = new SymbolTable(Map.empty, Map.empty, 0)

  def deserialize(in: DataInputStream): SymbolTable = {
    import ByteSerializers._
    import EnhancedStreams._

    var tokensToValues: Map[Int, Any] = Map.empty

    val size = in.readInt()

    for (i <- 1 to size) {
      val token = in.readInt()
      val valueBytes = in.readLengthValue()
      val value = deserializeValue(valueBytes)

      tokensToValues = tokensToValues + (token -> value)
    }

    val valuesToTokens = tokensToValues.map { case (token, value) => (value, token) }
    new SymbolTable(tokensToValues, valuesToTokens, size)
  }
}
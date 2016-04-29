package smileyy.s3hred.column

import java.io.{DataInputStream, InputStream, OutputStream}

import com.google.common.io.CountingOutputStream
import com.typesafe.scalalogging.LazyLogging
import smileyy.s3hred.util.io.{ByteSerializers, EnhancedStreams}

/**
  * A column that a symbol table and tokens to represent values.
  */
class Tokenized extends ByteSerialization {
  override def reader(name: String, in: InputStream): ColumnReader = TokenizedColumnReader(name, in)
  override def writer: ColumnWriter = new TokenizedColumnWriter()
}
object Tokenized {
  def apply(): Tokenized = new Tokenized()
}

private class TokenizedColumnReader(val name: String, tokens: SymbolTable, in: DataInputStream)
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
private object TokenizedColumnReader {
  def apply(name: String, in: InputStream): TokenizedColumnReader = {
    val datastream = new DataInputStream(in)
    val tokens = SymbolTable.deserialize(datastream)
    new TokenizedColumnReader(name, tokens, datastream)
  }
}

private class TokenizedColumnWriter()
    extends ColumnWriter with LazyLogging {

  import EnhancedStreams._

  var tokens = SymbolTable.empty

  override def writeValue(out: OutputStream, value: Any): Unit = {
    tokens = tokens.tokenizeAnd(value) { token => out.writeInt(token) }
  }

  override def writeMetadata(out: OutputStream): Unit = {
    tokens.serializeTo(out)
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
        logger.debug(s"Token $token exists for $value")
        (token, this)
      case None =>
        val newToken = size
        logger.debug(s"Created token $newToken for $value")
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

    logger.debug(s"Serializing symbol table with $size elements")
    val counter = new CountingOutputStream(out)
    counter.writeInt(size)

    tokenToValue.foldLeft(counter) { case (stream, (token, value)) =>
      stream.writeInt(token).writeLengthValue(serializeValue(value))
    }

    logger.debug(s"Serialized symbol table to ${counter.getCount} bytes")
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
    logger.debug(s"Deserializing symbol table with $size elements")

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
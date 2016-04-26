package smileyy.s3hred.column.tokenized

import java.io.{DataInputStream, OutputStream}

import com.google.common.io.CountingOutputStream
import com.typesafe.scalalogging.LazyLogging
import smileyy.s3hred.util.io.{ByteSerializers, EnhancedStreams}

/**
  * A two-way mapping from Int<->Value
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

  private[tokenized] def deserialize(in: DataInputStream): SymbolTable = {
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
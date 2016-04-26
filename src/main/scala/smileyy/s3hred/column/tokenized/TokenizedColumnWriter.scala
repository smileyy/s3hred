package smileyy.s3hred.column.tokenized

import java.io.OutputStream

import com.google.common.io.CountingOutputStream
import com.typesafe.scalalogging.LazyLogging
import smileyy.s3hred.column.ColumnWriter
import smileyy.s3hred.util.io.EnhancedStreams

/**
  * Writes tokenized columns, where values are tokens mapped by a [[SymbolTable]]
  */
private[tokenized] class TokenizedColumnWriter()
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
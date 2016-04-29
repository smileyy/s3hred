package smileyy.s3hred.column

import java.io.DataInputStream

/**
  * The serialization format of a [[Column]].
  */
trait ColumnSerialization {
  def reader(in: DataInputStream): ColumnReader
  def writer: ColumnWriter

  override def toString: String = getClass.getSimpleName
}

trait ByteSerialization extends ColumnSerialization

package smileyy.s3hred.column

import java.io.{DataInputStream, DataOutputStream}

/**
  * The serialization format of a [[Column]].
  */
trait ColumnSerialization {
  def reader(data: DataInputStream, meta: DataInputStream): ColumnReader
  def writer(data: DataOutputStream, meta: DataOutputStream): ColumnWriter

  override def toString: String = getClass.getSimpleName
}

trait ByteSerialization extends ColumnSerialization

package smileyy.s3hred.column

import java.io.InputStream

case class Column(name: String, representation: Representation) {
  def reader(in: InputStream): ColumnReader = representation.reader(name, in)
  def writer: ColumnWriter = representation.writer
}

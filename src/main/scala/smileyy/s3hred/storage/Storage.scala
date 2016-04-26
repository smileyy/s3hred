package smileyy.s3hred.storage

import smileyy.s3hred.query.{Select, Where}

/**
  * Defines a [[smileyy.s3hred.Dataset]] storage mechanism
  */
trait Storage {
  def iterator(select: Select, where: Where): Iterator[Seq[Any]]
}

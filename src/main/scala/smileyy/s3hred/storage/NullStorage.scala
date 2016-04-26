package smileyy.s3hred.storage
import smileyy.s3hred.query.{Select, Where}

/**
  * A null-object [[Storage]] implementation, primarily used for testing.
  */
object NullStorage extends Storage {
  override def iterator(select: Select, where: Where): Iterator[Seq[Any]] = Iterator.empty
}

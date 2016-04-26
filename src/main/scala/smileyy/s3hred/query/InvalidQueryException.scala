package smileyy.s3hred.query

/**
  * Thrown when an invalid query is attempted.
  */
class InvalidQueryException(msg: String, cause: Throwable = null) extends RuntimeException(msg, cause)

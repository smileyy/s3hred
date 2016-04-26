package smileyy.s3hred.schema

/**
  * Thrown upon the attempted creation of an invalid schema.
  */
class InvalidSchemaException(msg: String, cause: Throwable = null) extends RuntimeException(msg, cause)

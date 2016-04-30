package smileyy.s3hred

import java.io.OutputStream

import com.google.common.io.CountingOutputStream
import com.typesafe.scalalogging.LazyLogging
import smileyy.s3hred.schema.DatasetSchema

trait DatasetBuilder {
  def close(): Dataset
}

trait RowAddingBuilder extends DatasetBuilder {
  /**
    * Adds the row to the dataset being built and returns itself
    *
    * @param row the row to add
    * @return the builder
    */
  def add(row: Seq[Any]): RowAddingBuilder
}

class ByRowBuilderDelegate(
    schema: DatasetSchema,
    datastreams: Seq[CountingOutputStream],
    metastreams: Seq[CountingOutputStream])
  extends LazyLogging {

  var numberOfRows = 0

  val writers = {
    (schema.columns, datastreams, metastreams).zipped.map { case (column, data, meta) =>
      column.writer(data, meta)
    }
  }

  def add(row: Seq[Any]): Unit = {
    writers.zip(row) foreach { case (writer, value) => writer.write(value) }
    numberOfRows += 1
  }

  def close(): Unit = {
    writers.foreach(_.close())
    (schema.columnNames, datastreams, metastreams).zipped.foreach { case (name, data, meta) =>
      data.close()
      logger.info(s"Wrote ${data.getCount} data bytes for column $name")
      meta.close()
      logger.info(s"Wrote ${meta.getCount} meta bytes for column $name")
    }
  }
}
object ByRowBuilderDelegate {
  def apply(
      schema: DatasetSchema,
      datastreams: Seq[OutputStream],
      metastreams: Seq[OutputStream]): ByRowBuilderDelegate = {

    val datacounters = datastreams.map(new CountingOutputStream(_))
    val metacounters = metastreams.map(new CountingOutputStream(_))
    new ByRowBuilderDelegate(schema, datacounters, metacounters)
  }
}
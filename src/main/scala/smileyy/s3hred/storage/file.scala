package smileyy.s3hred.storage
import java.io._
import java.nio.file.{Files, Path}
import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import smileyy.s3hred.column.{Column, ColumnReader}
import smileyy.s3hred.query.{Select, Where}
import smileyy.s3hred.row.RowIterator
import smileyy.s3hred.schema.DatasetSchema
import smileyy.s3hred.{ByRowBuilderDelegate, ByRowBuilderDelegate$, Dataset, RowAddingBuilder}

/**
  * Stores [[smileyy.s3hred.Dataset]]s in the filesystem
  */
class FileStorageSystem(storageSystemRoot: Path) extends StorageSystem {
  override def rowAdder(schema: DatasetSchema): RowAddingBuilder = {
    val datasetDir = storageSystemRoot.resolve(UUID.randomUUID.toString)
    Files.createDirectories(datasetDir)
    new FileRowBuilder(schema, datasetDir)
  }
}

private class FileStorage(
    schema: DatasetSchema,
    val totalNumberOfRows: Long,
    datafiles: Seq[File],
    metafiles: Seq[File])
  extends Storage {

  val columns = (schema.columnNames, datafiles, metafiles).zipped.map { case (name, data, meta) =>
    name -> (data, meta)
  }.toMap

  override def iterator(select: Select, where: Where): Iterator[Seq[Any]] = {
    val readers: Map[String, ColumnReader] = {
      val names = select.columns.toSet ++ where.columns
      names.map { name =>
        val column = schema.column(name)
        val (data, meta) = columns(name)
        val reader = column.reader(new FileInputStream(data), new FileInputStream(meta))
        name -> reader
      }.toMap
    }

    new RowIterator(totalNumberOfRows, readers, select, where)
  }
}

private class FileRowBuilder(schema: DatasetSchema, dir: Path) extends RowAddingBuilder with LazyLogging {
  val columns = schema.columns

  val datafiles: Seq[File] = columns.map(fileForColumn(_, ".data"))
  val metafiles: Seq[File] = columns.map(fileForColumn(_, ".meta"))

  val delegate = ByRowBuilderDelegate(
    schema,
    datafiles.map(new FileOutputStream(_)),
    metafiles.map(new FileOutputStream(_))
  )

  override def add(row: Seq[Any]): RowAddingBuilder = {
    delegate.add(row)
    this
  }

  override def close(): Dataset = {
    delegate.close()

    logger.info(s"Created dataset in ${dir.toAbsolutePath}")
    new Dataset(schema, new FileStorage(schema, delegate.numberOfRows, datafiles, metafiles))
  }

  private def fileForColumn(column: Column, suffix: String) = dir.resolve(column.name + suffix).toFile
}

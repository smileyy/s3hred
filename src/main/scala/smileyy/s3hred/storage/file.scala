package smileyy.s3hred.storage
import java.io._
import java.nio.file.{Files, Path, Paths}
import java.util.UUID

import smileyy.s3hred.column.{ColumnReader, ColumnWriter}
import smileyy.s3hred.query.{Select, Where}
import smileyy.s3hred.row.RowIterator
import smileyy.s3hred.{Dataset, RowAddingBuilder}
import smileyy.s3hred.schema.DatasetSchema

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
    columnFiles: Seq[(File, File)])
  extends Storage {

  val columns = schema.columnNames.zip(columnFiles).map { case (name, tuple) =>
    name -> tuple
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

private class FileRowBuilder(schema: DatasetSchema, dir: Path) extends RowAddingBuilder {
  var numberOfRows: Long = 0

  val columns = schema.columns
  val files: Seq[(File, File)] = columns.map { column =>
    def file(suffix: String) = dir.resolve(column.name + suffix).toFile
    (file(".data"), file(".meta"))
  }

  val writers: Seq[ColumnWriter] = columns.zip(files).map { case (column, (data, meta)) =>
    column.writer(new FileOutputStream(data), new FileOutputStream(meta))
  }

  override def add(row: Seq[Any]): RowAddingBuilder = {
    writers.zip(row).foreach { case (writer, value) =>
      writer.write(value)
    }
    numberOfRows += 1
    this
  }

  override def close(): Dataset = {
    writers.foreach(_.close())
    new Dataset(schema, new FileStorage(schema, numberOfRows, files))
  }
}

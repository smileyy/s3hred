package smileyy.s3hred.csv

import java.io._
import java.nio.charset.{Charset, StandardCharsets}

import org.apache.commons.csv.{CSVFormat, CSVParser}
import smileyy.s3hred.Dataset
import smileyy.s3hred.column.Column
import smileyy.s3hred.schema.DatasetSchema
import smileyy.s3hred.storage.StorageSystem
import smileyy.s3hred.util.csv.EnhancedCommonsCsv

/**
  * Factory for creating datasets from CSV
  */
class CsvDatasetBuilder(storage: StorageSystem, columnMapping: String => Column) {
  import EnhancedCommonsCsv._

  def fromFile(csv: File, charset: Charset = StandardCharsets.UTF_8): Dataset = {
    fromInputStream(new FileInputStream(csv), charset)
  }

  def fromInputStream(csv: InputStream, charset: Charset = StandardCharsets.UTF_8): Dataset = {
    fromReader(new InputStreamReader(csv, charset))
  }

  def fromReader(csv: Reader): Dataset = {
    val parser = new CSVParser(csv, CSVFormat.RFC4180.withHeader())
    val schema = new DatasetSchema(parser.columns.map(columnMapping))
    val builder = storage.rowAdder(schema)
    parser.recordIterator.foreach { record => builder.add(record.columnIterator.toSeq) }

    parser.close()
    builder.close()
  }
}
object CsvDatasetBuilder {
  def apply(storage: StorageSystem)(mapping: String => Column): CsvDatasetBuilder = {
    new CsvDatasetBuilder(storage, mapping)
  }
}


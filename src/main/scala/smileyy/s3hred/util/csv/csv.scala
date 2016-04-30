package smileyy.s3hred.util.csv

import org.apache.commons.csv.{CSVParser, CSVRecord}

import scala.collection.JavaConverters._

object EnhancedCommonsCsv {
  implicit class CsvParser(val parser: CSVParser) extends AnyVal {
    def columns: Seq[String] = {
      parser.getHeaderMap.asScala.toSeq.sortBy(_._2).map(_._1)
    }

    def recordIterator: Iterator[CSVRecord] = parser.iterator().asScala
  }

  implicit class CsvRecord(val record: CSVRecord) extends AnyVal {
    def columnIterator: Iterator[String] = record.iterator.asScala
  }
}

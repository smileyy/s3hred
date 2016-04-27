package smileyy.s3hred

trait DatasetBuilder {
  def close(): Dataset
}

trait RowAddingBuilder extends DatasetBuilder {
  /**
    * Adds the row to the dataset being built and returns itself
    * @param row the row to add
    * @return the builder
    */
  def add(row: Seq[Any]): RowAddingBuilder
}
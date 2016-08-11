package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.Dataset

trait Transformer[T] {
  def transform(sources: Sources): Dataset[T]
}
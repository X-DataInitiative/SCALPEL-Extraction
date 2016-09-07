package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.Dataset

trait DatasetTransformer[A, B] {
  def transform(input: Dataset[A]): Dataset[B]
}
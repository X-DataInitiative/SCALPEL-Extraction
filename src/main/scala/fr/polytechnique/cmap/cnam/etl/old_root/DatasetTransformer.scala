package fr.polytechnique.cmap.cnam.etl.old_root

import org.apache.spark.sql.Dataset

trait DatasetTransformer[A, B] {
  def transform(input: Dataset[A]): Dataset[B]
}
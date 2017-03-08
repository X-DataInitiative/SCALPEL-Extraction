package fr.polytechnique.cmap.cnam.etl.old_root

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.sources.Sources

trait Transformer[T] {
  def transform(sources: Sources): Dataset[T]
}
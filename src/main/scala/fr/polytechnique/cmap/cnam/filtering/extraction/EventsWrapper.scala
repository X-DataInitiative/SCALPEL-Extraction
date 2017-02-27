package fr.polytechnique.cmap.cnam.filtering.extraction

import org.apache.spark.sql.Dataset

trait EventsWrapper[T] {

  val data: Dataset[T]
}

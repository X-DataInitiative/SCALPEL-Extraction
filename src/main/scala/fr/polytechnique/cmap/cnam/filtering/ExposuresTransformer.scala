package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.utilities.functions._

trait ExposuresTransformer extends DatasetTransformer[FlatEvent, FlatEvent] {

  lazy val StudyStart = FilteringConfig.dates.studyStart

  def transform(input: Dataset[FlatEvent]): Dataset[FlatEvent]
}

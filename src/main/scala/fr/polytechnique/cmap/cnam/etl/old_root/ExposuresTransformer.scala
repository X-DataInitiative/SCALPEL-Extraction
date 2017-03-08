package fr.polytechnique.cmap.cnam.etl.old_root

import java.sql.Timestamp
import org.apache.spark.sql.Dataset

trait ExposuresTransformer extends DatasetTransformer[FlatEvent, FlatEvent] {

  val StudyStart: Timestamp = FilteringConfig.dates.studyStart

  def transform(input: Dataset[FlatEvent]): Dataset[FlatEvent]
}

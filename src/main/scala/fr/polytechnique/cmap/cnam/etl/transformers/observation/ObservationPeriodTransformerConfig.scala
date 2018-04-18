package fr.polytechnique.cmap.cnam.etl.transformers.observation

import java.sql.Timestamp
import java.time.LocalDate

trait ObservationPeriodTransformerConfig {
  val studyStart: LocalDate
  val studyEnd: LocalDate
}

object ObservationPeriodTransformerConfig {
  def apply(studyStart: Timestamp, studyEnd: Timestamp): ObservationPeriodTransformerConfig = {
    val _studyStart = studyStart
    val _studyEnd = studyEnd
    new ObservationPeriodTransformerConfig {
      override val studyStart: LocalDate = _studyStart.toLocalDateTime.toLocalDate
      override val studyEnd: LocalDate = _studyEnd.toLocalDateTime.toLocalDate
    }
  }
}

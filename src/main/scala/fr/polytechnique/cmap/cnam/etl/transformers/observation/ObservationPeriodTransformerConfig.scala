// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.observation

import java.sql.Timestamp
import java.time.LocalDate
import fr.polytechnique.cmap.cnam.etl.transformers.TransformerConfig

/**
  * Base definition of the config needed by the ObservationPeriod transformer.
  * If the transformer is needed by a study, it must define either a case class (if mutable) or an
  * object (if hardcoded) extending this class.
  * Important: It cannot be used directly by a study, because it's not compatible with pureconfig.
  */
class ObservationPeriodTransformerConfig(
  val studyStart: LocalDate,
  val studyEnd: LocalDate) extends TransformerConfig

object ObservationPeriodTransformerConfig {

  def apply(studyStart: Timestamp, studyEnd: Timestamp): ObservationPeriodTransformerConfig = {
    new ObservationPeriodTransformerConfig(
      studyStart.toLocalDateTime.toLocalDate,
      studyEnd.toLocalDateTime.toLocalDate
    )
  }
}

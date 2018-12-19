package fr.polytechnique.cmap.cnam.etl.extractors.hospitalstays

import java.sql.Timestamp
import fr.polytechnique.cmap.cnam.etl.extractors.ExtractorConfig
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class HospitalStayConfig(
  val minYear: Timestamp = makeTS(1900, 1, 1),
  val maxYear: Timestamp = makeTS(2020, 1, 1)) extends ExtractorConfig

object HospitalStayConfig {
  def apply(
    minYear: Timestamp = makeTS(1900, 1, 1),
    maxYear: Timestamp = makeTS(2020, 1, 1)): HospitalStayConfig = new HospitalStayConfig(minYear, maxYear)
}
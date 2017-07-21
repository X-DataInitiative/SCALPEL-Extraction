package fr.polytechnique.cmap.cnam.etl.extractors.tracklosses

import java.sql.Timestamp
import fr.polytechnique.cmap.cnam.etl.config.CaseClassConfig

case class TracklossesConfig(
    studyEnd: Timestamp,
    emptyMonths: Int = 4,
    tracklossMonthDelay: Int = 2)
  extends CaseClassConfig

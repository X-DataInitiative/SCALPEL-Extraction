// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.tracklosses

import java.sql.Timestamp
import me.danielpes.spark.datetime.Period
import me.danielpes.spark.datetime.implicits._
import fr.polytechnique.cmap.cnam.etl.config.CaseClassConfig

case class TracklossesConfig(
  studyEnd: Timestamp,
  emptyMonths: Period = 4.months,
  tracklossMonthDelay: Period = 2.months) extends CaseClassConfig

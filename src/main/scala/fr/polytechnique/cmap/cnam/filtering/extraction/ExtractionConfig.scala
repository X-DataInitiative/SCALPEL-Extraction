package fr.polytechnique.cmap.cnam.filtering.extraction

import java.sql.Timestamp
import fr.polytechnique.cmap.cnam.filtering.FilteringConfig

case class TracklossConfig(emptyMonths: Int, tracklossMonthDelay: Int)

case class ExtractionConfig(
  minAge: Int,
  maxAge: Int,
  minGender: Int,
  maxGender: Int,
  minYear: Int,
  maxYear: Int,
  minMonth: Int,
  maxMonth: Int,
  deathCode: Int,
  ageReferenceDate: java.sql.Timestamp,
  tracklossConfig: TracklossConfig,
  lastDate: java.sql.Timestamp

)

object ExtractionConfig {
  def init(): ExtractionConfig = ExtractionConfig(
    minAge = FilteringConfig.limits.minAge,
    maxAge = FilteringConfig.limits.maxAge,
    minGender = FilteringConfig.limits.minGender,
    maxGender = FilteringConfig.limits.maxGender,
    minYear = FilteringConfig.limits.minYear,
    maxYear = FilteringConfig.limits.maxYear,
    minMonth = FilteringConfig.limits.minMonth,
    maxMonth = FilteringConfig.limits.maxMonth,
    deathCode = FilteringConfig.mcoDeathCode,
    ageReferenceDate = FilteringConfig.dates.ageReference,
    tracklossConfig = TracklossConfig(
      FilteringConfig.tracklossDefinition.threshold,
      FilteringConfig.tracklossDefinition.delay
    ),
    lastDate = FilteringConfig.dates.studyEnd
  )
}

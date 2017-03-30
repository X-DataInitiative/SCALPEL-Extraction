package fr.polytechnique.cmap.cnam.etl.config

import fr.polytechnique.cmap.cnam.etl.old_root.FilteringConfig

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
  lastDate: java.sql.Timestamp,
  drugCategories: List[String],
  maxBoxQuantity: Int,
  mainDiagnosisCodes: List[String],
  linkedDiagnosisCodes: List[String],
  associatedDiagnosisCodes: List[String],
  imbDiagnosisCodes: List[String])

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
    lastDate = FilteringConfig.dates.studyEnd,
    drugCategories = FilteringConfig.drugCategories,
    maxBoxQuantity = FilteringConfig.limits.maxQuantityIrpha,
    mainDiagnosisCodes = FilteringConfig.mainDiagnosisCodes,
    linkedDiagnosisCodes = FilteringConfig.linkedDiagnosisCodes,
    associatedDiagnosisCodes = FilteringConfig.associatedDiagnosisCodes,
    imbDiagnosisCodes = FilteringConfig.imbDiagnosisCodes
  )
}

// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.patients

import java.sql.Timestamp
import java.time.LocalDate
import fr.polytechnique.cmap.cnam.etl.extractors.ExtractorConfig

class PatientsConfig(
  val ageReferenceDate: LocalDate,
  val minAge: Int = 18,
  val maxAge: Int = 120,
  val minYear: Int = 1900,
  val maxYear: Int = 2020,
  val minGender: Int = 1,
  val maxGender: Int = 2,
  val mcoDeathCode: Int = 9) extends ExtractorConfig

object PatientsConfig {

  def apply(ageReferenceDate: Timestamp): PatientsConfig = {
    apply(ageReferenceDate.toLocalDateTime.toLocalDate)
  }

  def apply(
    ageReferenceDate: LocalDate,
    minAge: Int = 18,
    maxAge: Int = 120,
    minYear: Int = 1900,
    maxYear: Int = 2020,
    minGender: Int = 1,
    maxGender: Int = 2,
    mcoDeathCode: Int = 9): PatientsConfig = {

    new PatientsConfig(ageReferenceDate, minAge, maxAge, minYear, maxYear, minGender, maxGender, mcoDeathCode)
  }
}
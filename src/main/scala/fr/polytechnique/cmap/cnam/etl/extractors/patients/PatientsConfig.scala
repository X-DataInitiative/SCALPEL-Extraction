package fr.polytechnique.cmap.cnam.etl.extractors.patients

import java.sql.Timestamp
import java.time.LocalDate

trait PatientsConfig {
  val ageReferenceDate: LocalDate
  val minAge: Int = 18
  val maxAge: Int = 120
  val minYear: Int = 1900
  val maxYear: Int = 2020
  val minGender: Int = 1
  val maxGender: Int = 2
  val mcoDeathCode: Int = 9
}

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

    val _ageReferenceDate = ageReferenceDate
    val _minAge = minAge
    val _maxAge = maxAge
    val _minYear = minYear
    val _maxYear = maxYear
    val _minGender = minGender
    val _maxGender = maxGender
    val _mcoDeathCode = mcoDeathCode

    new PatientsConfig {
      override val ageReferenceDate: LocalDate = _ageReferenceDate
      override val minAge: Int = _minAge
      override val maxAge: Int = _maxAge
      override val minYear: Int = _minYear
      override val maxYear: Int = _maxYear
      override val minGender: Int = _minGender
      override val maxGender: Int = _maxGender
      override val mcoDeathCode: Int = _mcoDeathCode
    }
  }
}
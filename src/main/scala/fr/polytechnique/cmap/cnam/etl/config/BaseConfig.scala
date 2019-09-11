package fr.polytechnique.cmap.cnam.etl.config

import java.time.LocalDate

class BaseConfig(
  val ageReferenceDate: LocalDate,
  val studyStart: LocalDate,
  val studyEnd: LocalDate)

package fr.polytechnique.cmap.cnam.etl.config

import java.time.LocalDate

trait BaseConfig {
  val ageReferenceDate: LocalDate
  val studyStart: LocalDate
  val studyEnd: LocalDate
}

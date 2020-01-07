// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.config

import java.time.LocalDate

case class BaseConfig(ageReferenceDate: LocalDate, studyStart: LocalDate, studyEnd: LocalDate)

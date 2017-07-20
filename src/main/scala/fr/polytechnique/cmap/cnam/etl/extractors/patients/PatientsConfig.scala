package fr.polytechnique.cmap.cnam.etl.extractors.patients

import java.sql.Timestamp
import fr.polytechnique.cmap.cnam.etl.config.CaseClassConfig

case class PatientsConfig(
    ageReferenceDate: Timestamp,
    minAge: Int = 18,
    maxAge: Int = 120,
    minYear: Int = 1900,
    maxYear: Int = 2020,
    minGender: Int = 1,
    maxGender: Int = 2,
    mcoDeathCode: Int = 9)
  extends CaseClassConfig

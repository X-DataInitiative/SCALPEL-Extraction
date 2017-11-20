package fr.polytechnique.cmap.cnam.etl.filters

import fr.polytechnique.cmap.cnam.etl.patients.Patient
import org.apache.spark.sql.Dataset

object PatientFilters {
  implicit def addImplicits(patients: Dataset[Patient]): PatientFiltersImplicits = new PatientFiltersImplicits(patients)
}
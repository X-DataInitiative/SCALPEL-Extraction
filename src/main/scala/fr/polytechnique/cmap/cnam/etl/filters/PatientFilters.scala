package fr.polytechnique.cmap.cnam.etl.filters

import fr.polytechnique.cmap.cnam.etl.patients.Patient
import org.apache.spark.sql.Dataset

/*
 * The architectural decisions regarding the patient filters can be found in the following page:
 * https://datainitiative.atlassian.net/wiki/spaces/CFC/pages/109051905/Architecture+decisions
 */
object PatientFilters {
  implicit def addImplicits(patients: Dataset[Patient]): PatientFiltersImplicits = {
    new PatientFiltersImplicits(patients)
  }
}
package fr.polytechnique.cmap.cnam.etl.old_root.exposures

import org.apache.spark.sql.DataFrame

// Todo: this should be extracted the ExposuresTransformer pipeline
trait PatientFilters {

  implicit def addFiltersImplicits(data: DataFrame): PatientFiltersImplicits = {
    new PatientFiltersImplicits(data)
  }
}

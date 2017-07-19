package fr.polytechnique.cmap.cnam.etl.extract.patients

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources

trait PatientsExtractor {
  def extract(config: ExtractionConfig, sources: Sources): Dataset[Patient]
}

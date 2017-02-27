package fr.polytechnique.cmap.cnam.filtering.extraction

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.filtering.{Patient, Sources}

trait PatientsExtractor {
  def extract(config: ExtractionConfig, sources: Sources): Dataset[Patient]
}

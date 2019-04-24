package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets

import org.apache.spark.sql.Dataset

class Diagnoses(config: DiagnosesConfig) {

  def extract(sources: Sources): Dataset[Event[Diagnosis]] = {

    NewImbDiagnosisExtractor.extract(sources, config.imbCodes.toSet)
  }
}

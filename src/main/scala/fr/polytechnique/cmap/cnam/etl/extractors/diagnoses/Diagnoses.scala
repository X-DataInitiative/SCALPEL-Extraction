package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event}
import fr.polytechnique.cmap.cnam.etl.sources.Sources

@deprecated
class Diagnoses(config: DiagnosesConfig) {

  def extract(sources: Sources): Dataset[Event[Diagnosis]] = {

    NewImbDiagnosisExtractor.extract(sources, config.imbCodes.toSet)
  }
}

package fr.polytechnique.cmap.cnam.etl.extract.diagnoses

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event}
import fr.polytechnique.cmap.cnam.etl.extract.EventsExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets

object Diagnoses extends EventsExtractor[Diagnosis] {

  def extract(
      config: ExtractionConfig,
      sources: Sources): Dataset[Event[Diagnosis]] = {

    val imbDiagnoses = ImbDiagnoses.extract(
      sources.irImb.get,
      config.imbDiagnosisCodes
    )
    val mcoDiagnoses = McoDiagnoses.extract(
      sources.pmsiMco.get,
      config.mainDiagnosisCodes,
      config.linkedDiagnosisCodes,
      config.associatedDiagnosisCodes
    )

    unionDatasets(imbDiagnoses, mcoDiagnoses)
  }
}

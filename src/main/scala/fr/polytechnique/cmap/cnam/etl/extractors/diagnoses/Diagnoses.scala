package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets

class Diagnoses(config: DiagnosesConfig) {

  def extract(sources: Sources): Dataset[Event[Diagnosis]] = {

    val imbDiagnoses = ImbDiagnoses.extract(
      sources.irImb.get,
      config.imbCodes
    )
    val mcoDiagnoses = McoDiagnoses.extract(
      sources.pmsiMco.get,
      config.dpCodes,
      config.drCodes,
      config.daCodes
    )

    unionDatasets(imbDiagnoses, mcoDiagnoses)
  }
}

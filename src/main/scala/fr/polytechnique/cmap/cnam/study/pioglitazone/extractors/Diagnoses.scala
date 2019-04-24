package fr.polytechnique.cmap.cnam.study.pioglitazone.extractors

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event}
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions

class Diagnoses(config: DiagnosesConfig) {

  def extract(sources: Sources): Dataset[Event[Diagnosis]] = {

    val mainDiag = NewMainDiagnosisExtractor.extract(sources, config.dpCodes.toSet)
    val linkedDiag = NewLinkedDiagnosisExtractor.extract(sources, config.drCodes.toSet)
    val associatedDiag = NewAssociatedDiagnosisExtractor.extract(sources, config.daCodes.toSet)
    val imbDiag = NewImbDiagnosisExtractor.extract(sources, config.imbCodes.toSet)
    functions.unionDatasets(mainDiag, linkedDiag, associatedDiag, imbDiag)
  }

}

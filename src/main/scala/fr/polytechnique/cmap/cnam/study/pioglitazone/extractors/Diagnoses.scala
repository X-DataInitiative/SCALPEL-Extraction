// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.pioglitazone.extractors

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event}
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.events.diagnoses._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions

class Diagnoses(config: DiagnosesConfig) {

  def extract(sources: Sources): Dataset[Event[Diagnosis]] = {

    val mainDiag = McoMainDiagnosisExtractor(SimpleExtractorCodes(config.dpCodes)).extract(sources)
    val linkedDiag = McoLinkedDiagnosisExtractor(SimpleExtractorCodes(config.drCodes)).extract(sources)
    val associatedDiag = McoAssociatedDiagnosisExtractor(SimpleExtractorCodes(config.daCodes)).extract(sources)
    //val imbDiag = ImbDiagnosisExtractor.extract(sources, config.imbCodes.toSet)
    functions.unionDatasets(mainDiag, linkedDiag, associatedDiag)//, imbDiag)
  }

}

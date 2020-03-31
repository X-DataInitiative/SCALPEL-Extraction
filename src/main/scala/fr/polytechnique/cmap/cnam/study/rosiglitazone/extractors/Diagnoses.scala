// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.rosiglitazone.extractors

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event}
import fr.polytechnique.cmap.cnam.etl.extractors.BaseExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions

class Diagnoses(config: DiagnosesConfig) {

  def extract(sources: Sources): Dataset[Event[Diagnosis]] = {

    val mainDiag = McoMainDiagnosisExtractor(BaseExtractorCodes(config.dpCodes)).extract(sources)
    val linkedDiag = McoLinkedDiagnosisExtractor(BaseExtractorCodes(config.drCodes)).extract(sources)
    val associatedDiag = McoAssociatedDiagnosisExtractor(BaseExtractorCodes(config.daCodes)).extract(sources)
    functions.unionDatasets(mainDiag, linkedDiag, associatedDiag)
  }

}

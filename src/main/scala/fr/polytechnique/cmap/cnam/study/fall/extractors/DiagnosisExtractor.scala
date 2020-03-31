// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.extractors

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event}
import fr.polytechnique.cmap.cnam.etl.extractors.BaseExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets

class DiagnosisExtractor(config: DiagnosesConfig) {

  def extract(sources: Sources): Dataset[Event[Diagnosis]] = {

    val mainDiag = MainDiagnosisFallExtractor(BaseExtractorCodes(config.dpCodes)).extract(sources)
    val linkedDiag = LinkedDiagnosisFallExtractor(BaseExtractorCodes(config.drCodes)).extract(sources)
    val dasDiag = AssociatedDiagnosisFallExtractor(BaseExtractorCodes(config.daCodes)).extract(sources)

    unionDatasets(mainDiag, linkedDiag, dasDiag)
  }

}

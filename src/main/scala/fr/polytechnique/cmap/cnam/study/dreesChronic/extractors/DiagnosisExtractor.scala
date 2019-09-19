package fr.polytechnique.cmap.cnam.study.dreesChronic.extractors

import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event}
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets
import org.apache.spark.sql.Dataset

class DiagnosisExtractor(config: DiagnosesConfig) {

  def extract(sources: Sources): Dataset[Event[Diagnosis]] = {

    val mcoMainDiag = McoMainDiagnosisExtractor.extract(sources, config.dpCodes.toSet)
    val mcoLinkedDiag = McoLinkedDiagnosisExtractor.extract(sources, config.drCodes.toSet)
    val ssrMainDiag = SsrMainDiagnosisExtractor.extract(sources, config.dpCodes.toSet)
    val ssrLinkedDiag = SsrLinkedDiagnosisExtractor.extract(sources, config.drCodes.toSet)

    unionDatasets(mcoMainDiag, mcoLinkedDiag, ssrMainDiag, ssrLinkedDiag)
  }

}

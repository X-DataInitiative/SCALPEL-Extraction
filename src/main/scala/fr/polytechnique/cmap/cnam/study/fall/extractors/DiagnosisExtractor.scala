// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.extractors

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event}
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets

class DiagnosisExtractor(config: DiagnosesConfig) {

  def extract(sources: Sources): Dataset[Event[Diagnosis]] = {

    val mainDiag = McoMainDiagnosisExtractor.extract(sources, config.dpCodes.toSet)
    val linkedDiag = McoLinkedDiagnosisExtractor.extract(sources, config.drCodes.toSet)
    val dasDiag = McoAssociatedDiagnosisExtractor.extract(sources, config.daCodes.toSet)

    unionDatasets(mainDiag, linkedDiag, dasDiag)
  }

}

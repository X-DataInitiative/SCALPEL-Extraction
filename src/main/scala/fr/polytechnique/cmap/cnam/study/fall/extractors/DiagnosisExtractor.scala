// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.extractors

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event}
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets

class DiagnosisExtractor(config: DiagnosesConfig) {

  def extract(sources: Sources): Dataset[Event[Diagnosis]] = {

    val mainDiag = MainDiagnosisFallExtractor.extract(sources, config.dpCodes.toSet)
    val linkedDiag = LinkedDiagnosisFallExtractor.extract(sources, config.drCodes.toSet)
    val dasDiag = AssociatedDiagnosisFallExtractor.extract(sources, config.daCodes.toSet)

    unionDatasets(mainDiag, linkedDiag, dasDiag)
  }

}

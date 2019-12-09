// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.extractors

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event}
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses.{ImbDiagnosisExtractor, McoLinkedDiagnosisExtractor, McoMainDiagnosisExtractor}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets

object EpilepticsExtractor {
  def extract(sources: Sources): Dataset[Event[Diagnosis]] = {

    val mainDiag = McoMainDiagnosisExtractor.extract(sources, Set("G40"))
    val linkedDiag = McoLinkedDiagnosisExtractor.extract(sources, Set("G40"))
    val imbDiag = ImbDiagnosisExtractor.extract(sources, Set("G40"))

    unionDatasets(mainDiag, linkedDiag, imbDiag)
  }
}

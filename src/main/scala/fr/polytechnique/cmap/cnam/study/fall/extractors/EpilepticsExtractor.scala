// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.extractors

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event}
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.events.diagnoses.{ImbCimDiagnosisExtractor, McoLinkedDiagnosisExtractor, McoMainDiagnosisExtractor}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets

object EpilepticsExtractor {
  def extract(sources: Sources): Dataset[Event[Diagnosis]] = {

    val mainDiag = McoMainDiagnosisExtractor(SimpleExtractorCodes(List("G40"))).extract(sources)
    val linkedDiag = McoLinkedDiagnosisExtractor(SimpleExtractorCodes(List("G40"))).extract(sources)
    val imbDiag = ImbCimDiagnosisExtractor(SimpleExtractorCodes(List("G40"))).extract(sources)

    unionDatasets(mainDiag, linkedDiag, imbDiag)
  }
}

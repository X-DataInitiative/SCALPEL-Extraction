package fr.polytechnique.cmap.cnam.study.bulk.extractors

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event}
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses.{AssociatedDiagnosisExtractor, LinkedDiagnosisExtractor, MainDiagnosisExtractor}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.study.bulk.codes.DiabetesCodes
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets

object DiabetesDiagnosisExtractor extends DiabetesCodes {
  def extract(sources: Sources): Dataset[Event[Diagnosis]] = {
    val codes = diabetesPMSICim10 ++ diabetesComplicationsPMSICim10
    val mainDiag = MainDiagnosisExtractor.extract(sources, codes)
    val linkedDiag = LinkedDiagnosisExtractor.extract(sources, codes)
    val dasDiag = AssociatedDiagnosisExtractor.extract(sources, codes)

    unionDatasets(mainDiag, linkedDiag, dasDiag)
  }
}

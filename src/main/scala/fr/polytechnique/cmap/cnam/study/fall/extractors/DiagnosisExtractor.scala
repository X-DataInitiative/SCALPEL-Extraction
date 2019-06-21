package fr.polytechnique.cmap.cnam.study.fall.extractors

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event, _}
import fr.polytechnique.cmap.cnam.etl.extractors._
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses._
import fr.polytechnique.cmap.cnam.etl.sources.Sources

class DiagnosisExtractor(config: DiagnosesConfig) extends Serializable with MCOSourceInfo {

  def extract(sources: Sources): Dataset[Event[Diagnosis]] = {

    new MCOSourceExtractor(sources, List(
        new MCODiagnosisEventExtractor(MCOCols.DP, config.dpCodes, MainDiagnosis),
        new MCODiagnosisEventExtractor(MCOCols.DR, config.drCodes, LinkedDiagnosis),
        new MCODiagnosisEventExtractor(MCOCols.DA, config.daCodes, AssociatedDiagnosis)
      )
    ).extract()
  }
}

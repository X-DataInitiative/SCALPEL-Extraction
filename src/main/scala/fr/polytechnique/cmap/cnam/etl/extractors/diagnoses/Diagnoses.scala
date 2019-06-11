package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event, _}
import fr.polytechnique.cmap.cnam.etl.extractors._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets
import org.apache.spark.sql.Dataset


class Diagnoses(config: DiagnosesConfig) extends Serializable with MCOSourceInfo {

  def extract(sources: Sources): Dataset[Event[Diagnosis]] = {

    val imbDiagnoses : Dataset[Event[Diagnosis]] = new IMBSourceExtractor().extract(
      sources.irImb.get,
      List(new IMBEventExtractor(config.imbCodes))
    )

    val mcoDiagnoses : Dataset[Event[Diagnosis]] = new MCOSourceExtractor().extract(
      sources.mco.get, List(
        new MCODiagnosisEventExtractor(MCOCols.DP, config.dpCodes, MainDiagnosis),
        new MCODiagnosisEventExtractor(MCOCols.DR, config.drCodes, LinkedDiagnosis),
        new MCODiagnosisEventExtractor(MCOCols.DA, config.daCodes, AssociatedDiagnosis)
      )
    )

    unionDatasets(imbDiagnoses, mcoDiagnoses).distinct()
  }
}

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
    val mcoAssociatedDiag = McoAssociatedDiagnosisExtractor.extract(sources, config.daCodes.toSet)
    val ssrMainDiag = SsrMainDiagnosisExtractor.extract(sources, config.dpCodes.toSet)
    val ssrLinkedDiag = SsrLinkedDiagnosisExtractor.extract(sources, config.drCodes.toSet)
    val ssrAssociatedDiag = SsrAssociatedDiagnosisExtractor.extract(sources, config.daCodes.toSet)
    val hadMainDiag = HadMainDiagnosisExtractor.extract(sources, config.dpCodes.toSet)
    val hadAssociatedDiag = HadAssociatedDiagnosisExtractor.extract(sources, config.daCodes.toSet)
    val irImbDiag = ImbDiagnosisExtractor.extract(sources, config.imbCodes.toSet)

    unionDatasets(
      mcoMainDiag,
      mcoLinkedDiag,
      mcoAssociatedDiag,
      ssrMainDiag,
      ssrLinkedDiag,
      ssrAssociatedDiag,
      hadMainDiag,
      hadAssociatedDiag,
      irImbDiag)
  }
}

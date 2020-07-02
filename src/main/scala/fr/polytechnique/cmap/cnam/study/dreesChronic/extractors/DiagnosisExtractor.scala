package fr.polytechnique.cmap.cnam.study.dreesChronic.extractors

import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event}
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.events.diagnoses._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets
import org.apache.spark.sql.Dataset

class DiagnosisExtractor(config: DiagnosesConfig) {

  def extract(sources: Sources): Dataset[Event[Diagnosis]] = {

    val mcoMainDiag = McoMainDiagnosisExtractor(SimpleExtractorCodes(config.dpCodes)).extract(sources)
    val mcoLinkedDiag = McoLinkedDiagnosisExtractor(SimpleExtractorCodes(config.drCodes)).extract(sources)
    val mcoAssociatedDiag = McoAssociatedDiagnosisExtractor(SimpleExtractorCodes(config.daCodes)).extract(sources)
    val ssrMainDiag = SsrMainDiagnosisExtractor(SimpleExtractorCodes(config.dpCodes)).extract(sources)
    val ssrLinkedDiag = SsrLinkedDiagnosisExtractor(SimpleExtractorCodes(config.drCodes)).extract(sources)
    val ssrAssociatedDiag = SsrAssociatedDiagnosisExtractor(SimpleExtractorCodes(config.daCodes)).extract(sources)
    val ssrTakingOverPurpose = SsrTakingOverPurposeExtractor(SimpleExtractorCodes.empty).extract(sources)
    val hadMainDiag = HadMainDiagnosisExtractor(SimpleExtractorCodes(config.dpCodes)).extract(sources)
    val hadAssociatedDiag = HadAssociatedDiagnosisExtractor(SimpleExtractorCodes(config.daCodes)).extract(sources)

    //val irImbDiag = ImbDiagnosisExtractor.extract(sources, config.imbCodes.toSet)

    unionDatasets(
      mcoMainDiag,
      mcoLinkedDiag,
      mcoAssociatedDiag,
      ssrMainDiag,
      ssrLinkedDiag,
      ssrAssociatedDiag,
      hadMainDiag,
      hadAssociatedDiag,
      ssrTakingOverPurpose
      //irImbDiag
    )
  }
}

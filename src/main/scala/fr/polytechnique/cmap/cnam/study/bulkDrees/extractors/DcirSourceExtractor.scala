// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.bulkDrees.extractors

import fr.polytechnique.cmap.cnam.etl.events.{Drug, MedicalAct, NgapAct, PractitionerClaimSpeciality}
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.events.acts.{DcirBiologyActExtractor, DcirMedicalActExtractor}
import fr.polytechnique.cmap.cnam.etl.extractors.events.drugs.{DrugConfig, DrugExtractor}
import fr.polytechnique.cmap.cnam.etl.extractors.events.ngapacts.{DcirNgapActExtractor, NgapActConfig, NgapWithNatClassConfig}
import fr.polytechnique.cmap.cnam.etl.extractors.events.prestations.{MedicalPractitionerClaimExtractor, NonMedicalPractitionerClaimExtractor}

class DcirSourceExtractor(
  override val path: String,
  override val saveMode: String,
  val drugConfig: DrugConfig) extends SourceExtractor(path, saveMode) {
  override val sourceName: String = "DCIR"

  override val extractors = List(
    ExtractorSources[MedicalAct, SimpleExtractorCodes](
      DcirMedicalActExtractor(SimpleExtractorCodes.empty),
      List("ER_PRS_F", "ER_CAM_F", "ER_ETE_F"),
      "DCIR_MEDICAL_ACT"
    ),
    ExtractorSources[MedicalAct, SimpleExtractorCodes](
      DcirBiologyActExtractor(SimpleExtractorCodes.empty),
      List("ER_PRS_F", "ER_BIO_F", "ER_ETE_F"),
      "DCIR_BIOLOGICAL_ACT"
    ),
    ExtractorSources[Drug, DrugConfig](new DrugExtractor(drugConfig), List("ER_PRS_F", "IR_PHA_R"), "DRUG_PURCHASES"),
    ExtractorSources[NgapAct, NgapActConfig[NgapWithNatClassConfig]](
      new DcirNgapActExtractor(NgapActConfig(List.empty)),
      List("ER_PRS_F", "IR_NAT_V", "ER_ETE_F"),
      "DCIR_NGAP_ACTS"
    ),
    ExtractorSources[PractitionerClaimSpeciality, SimpleExtractorCodes](
      MedicalPractitionerClaimExtractor(SimpleExtractorCodes.empty),
      List("ER_PRS_F"),
      "DCIR_MEDICAL_PRACTIONNER"
    ),
    ExtractorSources[PractitionerClaimSpeciality, SimpleExtractorCodes](
      NonMedicalPractitionerClaimExtractor(SimpleExtractorCodes.empty),
      List("ER_PRS_F"),
      "DCIR_NON_MEDICAL_PRACTIONNER"
    )
  )
}

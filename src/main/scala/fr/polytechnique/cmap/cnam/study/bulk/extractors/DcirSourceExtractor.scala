// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.bulk.extractors

import fr.polytechnique.cmap.cnam.etl.events.{Drug, MedicalAct, NgapAct, PractitionerClaimSpeciality}
import fr.polytechnique.cmap.cnam.etl.extractors.acts.{DcirBiologyActExtractor, DcirMedicalActExtractor}
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.{DrugConfig, DrugExtractor}
import fr.polytechnique.cmap.cnam.etl.extractors.ngapacts.{DcirNgapActExtractor, NgapActConfig}
import fr.polytechnique.cmap.cnam.etl.extractors.prestations.{MedicalPractitionerClaimExtractor, NonMedicalPractitionerClaimExtractor}

class DcirSourceExtractor(
  override val path: String,
  override val saveMode: String,
  val drugConfig: DrugConfig) extends SourceExtractor(path, saveMode) {
  override val sourceName: String = "DCIR"

  override val extractors = List(
    ExtractorSources[MedicalAct](DcirMedicalActExtractor, List("ER_PRS_F", "ER_CAM_F", "ER_ETE_F"), "DCIR_MEDICAL_ACT"),
    ExtractorSources[MedicalAct](
      DcirBiologyActExtractor,
      List("ER_PRS_F", "ER_BIO_F", "ER_ETE_F"),
      "DCIR_BIOLOGICAL_ACT"
    ),
    ExtractorSources[Drug](new DrugExtractor(drugConfig), List("ER_PRS_F", "IR_PHA_R"), "DRUG_PURCHASES"),
    ExtractorSources[NgapAct](
      new DcirNgapActExtractor(NgapActConfig(List.empty)),
      List("ER_PRS_F", "IR_NAT_V", "ER_ETE_F"),
      "DCIR_NGAP_ACTS"
    ),
    ExtractorSources[PractitionerClaimSpeciality](
      MedicalPractitionerClaimExtractor,
      List("ER_PRS_F"),
      "DCIR_MEDICAL_PRACTIONNER"
    ),
    ExtractorSources[PractitionerClaimSpeciality](
      NonMedicalPractitionerClaimExtractor,
      List("ER_PRS_F"),
      "DCIR_NON_MEDICAL_PRACTIONNER"
    )
  )
}

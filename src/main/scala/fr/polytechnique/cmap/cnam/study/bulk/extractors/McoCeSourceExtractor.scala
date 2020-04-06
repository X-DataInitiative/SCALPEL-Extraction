// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.bulk.extractors

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.acts.McoCeActExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.hospitalstays.McoceEmergenciesExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.ngapacts.{McoCeFbstcNgapActExtractor, McoCeFcstcNgapActExtractor, NgapActConfig}
import fr.polytechnique.cmap.cnam.etl.extractors.prestations.{McoCeFbstcSpecialtyExtractor, McoCeFcstcSpecialtyExtractor}

class McoCeSourceExtractor(override val path: String, override val saveMode: String) extends SourceExtractor(
  path,
  saveMode
) {
  override val sourceName: String = "MCO_CE"
  override val extractors = List(
    ExtractorSources[MedicalAct](McoCeActExtractor, List("MCO_CSTC", "MCO_FMSTC"), "MCO_CE_CCAM_ACTS"),
    ExtractorSources[NgapAct](
      new McoCeFbstcNgapActExtractor(NgapActConfig(List.empty)),
      List("MCO_CSTC", "MCO_FBSTC"),
      "MCO_CE_FBSTC_NGAP_ACTS"
    ),
    ExtractorSources[NgapAct](
      new McoCeFcstcNgapActExtractor(NgapActConfig(List.empty)),
      List("MCO_CSTC", "MCO_FCSTC"),
      "MCO_CE_FCSTC_NGAP_ACTS"
    ),
    ExtractorSources[PractitionerClaimSpeciality](
      McoCeFbstcSpecialtyExtractor,
      List("MCO_CSTC", "MCO_FBSTC"),
      "MCO_CE_FBSTC_PRACTITIONER_SPECIALITY"
    ),
    ExtractorSources[PractitionerClaimSpeciality](
      McoCeFcstcSpecialtyExtractor,
      List("MCO_CSTC", "MCO_FCSTC"),
      "MCO_CE_FCSTC_PRACTITIONER_SPECIALITY"
    ),
    ExtractorSources[HospitalStay](McoceEmergenciesExtractor, List("MCO_CSTC", "MCO_FBSTC"), "MCO_CE_EMERGENCY_VISIT")
  )
}

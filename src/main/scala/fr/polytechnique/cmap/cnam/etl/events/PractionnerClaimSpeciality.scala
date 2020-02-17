// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.apache.spark.sql.Row

trait PractitionerClaimSpeciality extends AnyEvent with EventBuilder {

  val category: EventCategory[PractitionerClaimSpeciality]

  def apply(patientID: String, groupID: String, pfsSpe: String, date: Timestamp): Event[PractitionerClaimSpeciality] = {
    Event(patientID, category, groupID, pfsSpe, 0.0, date, None)
  }
}

object MedicalPractitionerClaim extends PractitionerClaimSpeciality {
  override val category: EventCategory[PractitionerClaimSpeciality] = "medical_practitioner_claim"
}

object NonMedicalPractitionerClaim extends PractitionerClaimSpeciality {
  override val category: EventCategory[PractitionerClaimSpeciality] = "non_medical_practitioner_claim"
}

object McoCeFbstcMedicalPractitionerClaim extends PractitionerClaimSpeciality {
  override val category: EventCategory[PractitionerClaimSpeciality] = "mco_ce__fbstc_practitioner_claim"
}

object McoCeFcstcMedicalPractitionerClaim extends PractitionerClaimSpeciality {
  override val category: EventCategory[PractitionerClaimSpeciality] = "mco_ce__fcstc_practitioner_claim"
}

object SsrCeFbstcMedicalPractitionerClaim extends PractitionerClaimSpeciality {
  override val category: EventCategory[PractitionerClaimSpeciality] = "ssr_ce__fbstc_practitioner_claim"
}

object SsrCeFcstcMedicalPractitionerClaim extends PractitionerClaimSpeciality {
  override val category: EventCategory[PractitionerClaimSpeciality] = "ssr_ce__fcstc_practitioner_claim"
}

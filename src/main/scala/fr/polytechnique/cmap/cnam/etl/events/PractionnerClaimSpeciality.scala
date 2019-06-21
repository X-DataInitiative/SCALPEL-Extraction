package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.apache.spark.sql.Row

trait PractitionerClaimSpeciality extends AnyEvent  {

  val category: EventCategory[PractitionerClaimSpeciality]

  def fromRow(
    r: Row,
    patientIDCol: String = "patientID",
    pfsIDCol: String = "groupID",
    pfsSpeCol: String = "code",
    dateCol: String = "eventDate"): Event[PractitionerClaimSpeciality] =
    apply(
      r.getAs[String](patientIDCol),
      r.getAs[String](pfsIDCol),
      r.getAs[String](pfsSpeCol),
      r.getAs[Timestamp](dateCol)
    )

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
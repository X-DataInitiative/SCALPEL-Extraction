package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.apache.spark.sql.Row

trait PrestationSpeciality extends AnyEvent with EventBuilder {

  val category: EventCategory[PrestationSpeciality]

  def apply(patientID: String, groupID: String, pfsSpe: String, date: Timestamp): Event[PrestationSpeciality] = {
    Event(patientID, category, groupID, pfsSpe, 0.0, date, None)
  }

  def fromRow(
    r: Row,
    patientIDCol: String = "patientID",
    pfsIDCol: String = "groupID",
    pfsSpeCol: String = "code",
    dateCol: String = "eventDate"): Event[PrestationSpeciality] =
    apply(
      r.getAs[String](patientIDCol),
      r.getAs[String](pfsIDCol),
      r.getAs[String](pfsSpeCol),
      r.getAs[Timestamp](dateCol)
    )
}

object MedicalPrestation extends PrestationSpeciality {
  override val category: EventCategory[PrestationSpeciality] = "medical_prestation"
}

object NonMedicalPrestation extends PrestationSpeciality {
  override val category: EventCategory[PrestationSpeciality] = "non_medical_prestation"
}
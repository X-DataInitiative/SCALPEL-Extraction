package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.apache.spark.sql.Row

trait MedicalTakeOverReason extends AnyEvent with EventBuilder {

  override val category: EventCategory[MedicalTakeOverReason]

  def fromRow(
    r: Row,
    patientIDCol: String = "patientID",
    groupIDCol: String = "groupID",
    codeCol: String = "code",
    dateCol: String = "eventDate"): Event[MedicalTakeOverReason] = {
    this.apply(
      r.getAs[String](patientIDCol),
      r.getAs[String](groupIDCol),
      r.getAs[String](codeCol),
      r.getAs[Timestamp](dateCol)
    )
  }

  def apply(patientID: String, groupID: String, code: String, date: Timestamp): Event[MedicalTakeOverReason] = {
    Event(patientID, category, groupID, code, 0.0, date, None)
  }
}

object HadMainTakeOver extends MedicalTakeOverReason {
  val category: EventCategory[MedicalTakeOverReason] = "had_main_take_over_reason"
}

object HadAssociatedTakeOver extends MedicalTakeOverReason {
  val category: EventCategory[MedicalTakeOverReason] = "had_associated_take_over_reason"
}
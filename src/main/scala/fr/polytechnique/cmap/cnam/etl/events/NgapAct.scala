package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp

import org.apache.spark.sql.Row

object NgapAct extends NgapAct

trait NgapAct extends AnyEvent with EventBuilder {

  override val category: EventCategory[NgapAct] = "ngap_act"

  def apply(patientID: String, groupID: String, coefficientAct: String, date: Timestamp): Event[NgapAct] = {
    Event(patientID, category, groupID, coefficientAct, 0.0, date, None)
  }

  def fromRow(
               r: Row,
               patientIDCol: String = "patientID",
               pfsIDCol: String = "groupID",
               coefficientAct: String = "code",
               dateCol: String = "eventDate"): Event[NgapAct] =
    apply(
      r.getAs[String](patientIDCol),
      r.getAs[String](pfsIDCol),
      r.getAs[String](coefficientAct),
      r.getAs[Timestamp](dateCol)
    )
}


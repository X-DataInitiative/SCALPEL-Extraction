package fr.polytechnique.cmap.cnam.etl.events.acts

import java.sql.Timestamp

import org.apache.spark.sql.Row

import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event, EventCategory}

trait MedicalAct extends AnyEvent

trait MedicalActBuilder extends MedicalAct with Serializable {

  val category: EventCategory[MedicalAct]

  def apply(patientID: String, code: String, date: Timestamp): Event[MedicalAct] =
    Event(patientID, category, code, 0.0, date, None)

  def fromRow(
               r: Row,
               patientIDCol: String = "patientID",
               codeCol: String = "code",
               dateCol: String = "eventDate"): Event[MedicalAct] = {

    apply(
      r.getAs[String](patientIDCol),
      r.getAs[String](codeCol),
      r.getAs[Timestamp](dateCol)
    )
  }
}

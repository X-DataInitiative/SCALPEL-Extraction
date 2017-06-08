package fr.polytechnique.cmap.cnam.etl.events.acts

import java.sql.Timestamp
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events._

trait MedicalActBuilder extends EventBuilder with AnyEvent with Serializable {

  val category: EventCategory[MedicalAct]

  def apply(patientID: String, groupID: String, code: String, date: Timestamp): Event[MedicalAct] = {
    Event(patientID, category, groupID, code, 0.0, date, None)
  }

  def fromRow(
      r: Row,
      patientIDCol: String = "patientID",
      groupIDCol: String = "groupID",
      codeCol: String = "code",
      dateCol: String = "eventDate"): Event[MedicalAct] = {

    apply(
      r.getAs[String](patientIDCol),
      r.getAs[String](groupIDCol),
      r.getAs[String](codeCol),
      r.getAs[Timestamp](dateCol)
    )
  }
}

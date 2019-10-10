package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.apache.spark.sql.Row

trait AldDiagnosis extends AnyEvent with EventBuilder {

  val category: EventCategory[AldDiagnosis]

  def fromRow(
     r: Row,
     patientIDCol: String,
     codeCol: String,
     dateCol: String,
     endCol: String): Event[AldDiagnosis] = {
    apply(
      r.getAs[String](patientIDCol),
      r.getAs[String](codeCol),
      r.getAs[Timestamp](dateCol),
      r.getAs[Timestamp](endCol))
  }

  def apply(
     patientID: String,
     code: String,
     date: Timestamp,
     endDate: Timestamp): Event[AldDiagnosis] = {
    Event(patientID, category, groupID = "NA", code, 0.0, date, Some(endDate))
  }
}

object ImbDiagnosis extends AldDiagnosis {
  override val category: EventCategory[AldDiagnosis] = "imb_diagnosis"
}

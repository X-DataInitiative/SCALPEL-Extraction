package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events.Event.Columns._

trait HospitalStay extends AnyEvent with EventBuilder {

  override val category: EventCategory[HospitalStay] = "hospital_stay"

  def apply(patientID: String, hospitalID: String, start: Timestamp, end: Timestamp): Event[HospitalStay] =
    apply(patientID, hospitalID, hospitalID, 0D, start, Some(end))

  def fromRow(
    r: Row,
    patientIDCol: String = PatientID,
    hospitalIDCol: String = Value,
    startCol: String = Start,
    endCol: String = End): Event[HospitalStay] =
    apply(
      r.getAs[String](patientIDCol),
      r.getAs[String](hospitalIDCol),
      r.getAs[Timestamp](startCol),
      r.getAs[Timestamp](endCol)
    )
}

object HospitalStay extends HospitalStay
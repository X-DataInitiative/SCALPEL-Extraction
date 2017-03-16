package fr.polytechnique.cmap.cnam.filtering.events

import java.sql.Timestamp
import org.apache.spark.sql.Row

trait Exposure extends AnyEvent

object Exposure extends Exposure {

  val category: EventCategory[Exposure] = "exposure"

  def apply(
    patientID: String, molecule: String, weight: Double, start: Timestamp, end: Timestamp
  ): Event[Exposure] = Event(patientID, category, molecule, weight, start, Some(end))

  def fromRow(
      r: Row,
      patientIDCol: String = "patientID",
      nameCol: String = "name",
      weightCol: String = "weight",
      startCol: String = "start",
      endCol: String = "end"): Event[Exposure] = {

    Exposure(
      r.getAs[String](patientIDCol),
      r.getAs[String](nameCol),
      r.getAs[Double](weightCol),
      r.getAs[Timestamp](startCol),
      r.getAs[Timestamp](endCol)
    )
  }
}

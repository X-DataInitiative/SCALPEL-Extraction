package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.apache.spark.sql.Row

object Drug extends Drug

trait Drug extends Dispensation {

  override val category: EventCategory[Drug] = "drug"

  def fromRow(
    r: Row,
    patientIDCol: String = "patientID",
    nameCol: String = "name",
    dosageCol: String = "dosage",
    dateCol: String = "eventDate"): Event[Drug] = {

    Drug(
      r.getAs[String](patientIDCol),
      r.getAs[String](nameCol),
      r.getAs[Double](dosageCol),
      r.getAs[Timestamp](dateCol)
    )
  }

  def apply(patientID: String, name: String, dosage: Double, date: Timestamp): Event[Drug] =
    Event(patientID, category, groupID = "NA", name, dosage, date, None)
}

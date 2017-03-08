package fr.polytechnique.cmap.cnam.etl.events.molecules

import java.sql.Timestamp
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event, EventCategory}

trait Molecule extends AnyEvent

object Molecule extends Molecule {

  val category: EventCategory[Molecule] = "molecule"

  def apply(patientID: String, name: String, dosage: Double, date: Timestamp): Event[Molecule] =
    Event(patientID, category, name, dosage, date, None)

  def fromRow(
      r: Row,
      patientIDCol: String = "patientID",
      nameCol: String = "name",
      dosageCol: String = "dosage",
      dateCol: String = "eventDate"): Event[Molecule] = {

    Molecule(
      r.getAs[String](patientIDCol),
      r.getAs[String](nameCol),
      r.getAs[Double](dosageCol),
      r.getAs[Timestamp](dateCol)
    )
  }
}

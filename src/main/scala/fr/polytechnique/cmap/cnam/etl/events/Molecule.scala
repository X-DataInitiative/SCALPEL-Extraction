// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.apache.spark.sql.Row

object Molecule extends Molecule

trait Molecule extends Dispensation with EventBuilder {

  override val category: EventCategory[Molecule] = "molecule"

  def apply(patientID: String, name: String, dosage: Double, date: Timestamp): Event[Molecule] =
    Event(patientID, category, groupID = "NA", name, dosage, date, None)
}


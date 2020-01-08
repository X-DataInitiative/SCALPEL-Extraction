// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.apache.spark.sql.Row

object Drug extends Drug

trait Drug extends Dispensation with EventBuilder {

  override val category: EventCategory[Drug] = "drug"

  def apply(patientID: String, name: String, dosage: Double, groupID: String, date: Timestamp): Event[Drug] =
    Event(patientID, category, groupID, name, dosage, date, None)
}
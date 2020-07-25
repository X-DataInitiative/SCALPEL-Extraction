// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp

/**
  * [[Event]] that combines [[Drug]]s to form a Prescription.
  */
trait DrugPrescription extends Dispensation with EventBuilder {
  override val category: EventCategory[DrugPrescription] = "drug_prescription"

  def apply(patientID: String, name: String, dosage: Double, groupID: String, date: Timestamp): Event[DrugPrescription] =
    Event(patientID, category, groupID, name, dosage, date, None)
}

object DrugPrescription extends DrugPrescription
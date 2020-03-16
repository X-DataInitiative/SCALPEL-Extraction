// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp

object DcirMedicalAppliance extends MedicalAppliance {
  override val category: EventCategory[MedicalAppliance] = "dcir_medical_appliance"
}

trait MedicalAppliance extends AnyEvent with EventBuilder {

  val category: EventCategory[MedicalAppliance] = "medical_appliance"

  def apply(
    patientID: String, groupID: String, lppCode: String, weight: Double, start: Timestamp, end: Timestamp
  ): Event[MedicalAppliance] = Event(patientID, category, groupID = groupID, lppCode, weight, start, Some(end))
}

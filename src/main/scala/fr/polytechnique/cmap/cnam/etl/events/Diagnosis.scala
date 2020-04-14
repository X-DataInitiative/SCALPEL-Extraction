// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp

trait Diagnosis extends AnyEvent with EventBuilder {

  val category: EventCategory[Diagnosis]

  def apply(patientID: String, code: String, date: Timestamp): Event[Diagnosis] = {
    Event(patientID, category, groupID = "NA", code, 0.0, date, None)
  }

  def apply(patientID: String, code: String, date: Timestamp, endDate: Option[Timestamp]): Event[Diagnosis] = {
    Event(patientID, category, groupID = "NA", code, 0.0, date, endDate)
  }

  def apply(patientID: String, groupID: String, code: String, date: Timestamp): Event[Diagnosis] = {
    Event(patientID, category, groupID, code, 0.0, date, None)
  }

  def apply(patientID: String, groupID: String, code: String, weight: Double, date: Timestamp): Event[Diagnosis] = {
    Event(patientID, category, groupID, code, weight, date, None)
  }

}

object McoMainDiagnosis extends Diagnosis {
  override val category: EventCategory[Diagnosis] = "mco_main_diagnosis"
}

object McoLinkedDiagnosis extends Diagnosis {
  override val category: EventCategory[Diagnosis] = "mco_linked_diagnosis"
}

object McoAssociatedDiagnosis extends Diagnosis {
  override val category: EventCategory[Diagnosis] = "mco_associated_diagnosis"
}

object HadMainDiagnosis extends Diagnosis {
  override val category: EventCategory[Diagnosis] = "had_main_diagnosis"
}

object HadAssociatedDiagnosis extends Diagnosis {
  override val category: EventCategory[Diagnosis] = "had_associated_diagnosis"
}

object SsrMainDiagnosis extends Diagnosis {
  override val category: EventCategory[Diagnosis] = "ssr_main_diagnosis"
}

object SsrAssociatedDiagnosis extends Diagnosis {
  override val category: EventCategory[Diagnosis] = "ssr_associated_diagnosis"
}

object SsrLinkedDiagnosis extends Diagnosis {
  override val category: EventCategory[Diagnosis] = "ssr_linked_diagnosis"
}

object SsrTakingOverPurpose extends Diagnosis {
  val category: EventCategory[Diagnosis] = "ssr_taking_over_purpose"
}

object ImbCcamDiagnosis extends Diagnosis {
  override val category: EventCategory[Diagnosis] = "imb_ccam_diagnosis"
}
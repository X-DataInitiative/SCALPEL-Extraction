package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.apache.spark.sql.Row

trait MedicalAct extends AnyEvent with EventBuilder {

  val category: EventCategory[MedicalAct]

  def apply(patientID: String, groupID: String, code: String, date: Timestamp): Event[MedicalAct] = {
    Event(patientID, category, groupID, code, 0.0, date, None)
  }

  def fromRow(
      r: Row,
      patientIDCol: String = "patientID",
      groupIDCol: String = "groupID",
      codeCol: String = "code",
      dateCol: String = "eventDate"): Event[MedicalAct] = {

    apply(
      r.getAs[String](patientIDCol),
      r.getAs[String](groupIDCol),
      r.getAs[String](codeCol),
      r.getAs[Timestamp](dateCol)
    )
  }
}

object DcirAct extends MedicalAct {
  val category: EventCategory[MedicalAct] = "dcir_act"

  object groupID {
    val PrivateAmbulatory = "private_ambulatory"
    val PublicAmbulatory = "public_ambulatory"
    val PrivateHospital = "private_hospital"
  }
}

object McoCCAMAct extends MedicalAct {
  val category: EventCategory[MedicalAct] = "mco_ccam_act"
}

object McoCIM10Act extends MedicalAct {
  val category: EventCategory[MedicalAct] = "mco_cim10_act"
}

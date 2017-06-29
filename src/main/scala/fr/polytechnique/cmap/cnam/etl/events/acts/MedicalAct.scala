package fr.polytechnique.cmap.cnam.etl.events.acts

import java.sql.Timestamp
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events._

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

object DcirCamAct extends MedicalAct {
  val category: EventCategory[MedicalAct] = "dcir_act"
}

object McoCamAct extends MedicalAct {
  val category: EventCategory[MedicalAct] = "mco_cam_act"
}

object McoCimAct extends MedicalAct {
  val category: EventCategory[MedicalAct] = "mco_cim_act"
}

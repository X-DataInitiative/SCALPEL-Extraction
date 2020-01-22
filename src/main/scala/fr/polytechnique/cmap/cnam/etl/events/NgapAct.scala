package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.apache.spark.sql.Row

trait NgapAct extends AnyEvent with EventBuilder {

   override val category: EventCategory[NgapAct] = "ngap_act"

  def apply(patientID: String, groupID: String, ngapCoefficient: String, date: Timestamp): Event[NgapAct] = {
    Event(patientID, category, groupID, ngapCoefficient, 0.0, date, None)
  }

  def fromRow(
     r: Row,
     patientIDCol: String = "patientID",
     pfsIDCol: String = "groupID",
     ngapCoefficient: String = "code",
     dateCol: String = "eventDate"): Event[NgapAct] =
    apply(
      r.getAs[String](patientIDCol),
      r.getAs[String](pfsIDCol),
      r.getAs[String](ngapCoefficient),
      r.getAs[Timestamp](dateCol)
    )
}


object DcirNgapAct extends NgapAct {
  override val category: EventCategory[Diagnosis] = "dcir_ngap_act"
}

object McoCeFbstcNgapAct extends NgapAct {
  override val category: EventCategory[Diagnosis] = "mco_ce_fbstc_act"
}

object McoCeFcstcNgapAct extends NgapAct {
  override val category: EventCategory[Diagnosis] = "mco_ce_fcstc_act"
}

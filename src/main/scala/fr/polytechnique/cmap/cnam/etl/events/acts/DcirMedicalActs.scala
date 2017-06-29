package fr.polytechnique.cmap.cnam.etl.events.acts

import java.sql.Timestamp
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import fr.polytechnique.cmap.cnam.etl.events.Event

private[acts] object DcirMedicalActs {

  final object ColNames {
    final val PatientID = "NUM_ENQ"
    final val CamCode = "ER_CAM_F__ACT_CAM_COD"
    final val Date = "EXE_SOI_DTD"
  }

  def medicalActFromRow(ccamCodes: Seq[String])(r: Row): Option[Event[MedicalAct]] = {

    val foundCode: Option[String] = ccamCodes.find {
      val idx = r.fieldIndex(ColNames.CamCode)
      !r.isNullAt(idx) && r.getString(idx).startsWith(_)
    }

    foundCode match {
      case None => None
      case Some(code) => Some(
        DcirAct(r.getAs[String](ColNames.PatientID), "dcir", code, r.getAs[Timestamp](ColNames.Date))
      )
    }
  }

  def extract(dcir: DataFrame, ccamCodes: Seq[String]): Dataset[Event[MedicalAct]] = {
    import dcir.sqlContext.implicits._
    dcir.flatMap(medicalActFromRow(ccamCodes))
  }
}

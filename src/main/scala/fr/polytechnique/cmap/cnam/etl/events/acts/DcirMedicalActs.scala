package fr.polytechnique.cmap.cnam.etl.events.acts

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import fr.polytechnique.cmap.cnam.etl.events.Event
import fr.polytechnique.cmap.cnam.util.datetime.implicits._

private[acts] object DcirMedicalActs {

  final object ColNames {
    final lazy val PatientID = "NUM_ENQ"
    final lazy val CamCode = "ER_CAM_F.CAM_PRS_IDE"
    final lazy val Date = "EXE_SOI_DTD"
    def allCols = List(PatientID, CamCode, Date)
  }

  def medicalActFromRow(ccamCodes: Seq[String])(r: Row): Option[Event[MedicalAct]] = {

    def noNulls: Boolean = {
      ColNames.allCols.forall { colName =>
        !r.isNullAt(r.fieldIndex(colName))
      }
    }

    val foundCode: Option[String] = ccamCodes.find {
      val idx = r.fieldIndex(ColNames.CamCode)
      noNulls && r.getString(idx).startsWith(_)
    }

    foundCode match {
      case None => None
      case Some(code) => Some(
        DcirAct(
          patientID = r.getAs[String](ColNames.PatientID),
          groupID = DcirAct.groupID,
          code = code,
          date = r.getAs[java.util.Date](ColNames.Date).toTimestamp
        )
      )
    }
  }

  def extract(dcir: DataFrame, ccamCodes: Seq[String]): Dataset[Event[MedicalAct]] = {
    import dcir.sqlContext.implicits._
    dcir.flatMap(medicalActFromRow(ccamCodes))
  }
}

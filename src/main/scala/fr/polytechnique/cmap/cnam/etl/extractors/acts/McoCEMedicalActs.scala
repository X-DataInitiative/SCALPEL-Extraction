package fr.polytechnique.cmap.cnam.etl.extractors.acts

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import fr.polytechnique.cmap.cnam.etl.events._

private[acts] object McoCEMedicalActs {

  final object ColNames {
    final lazy val PatientID = "NUM_ENQ"
    final lazy val CamCode = "MCO_FMSTC__CCAM_COD"
    final lazy val Date = "EXE_SOI_DTD"
    def allCols = List(PatientID, CamCode, Date)
  }

  val outputCols = List(
    col(ColNames.PatientID).as("patientID"),
    col(ColNames.CamCode).as("code"),
    col(ColNames.Date).cast(TimestampType).as("eventDate"),
    lit("ACE").as("groupID")
  )

  def correctCamCode(camCodes: Seq[String])(row: Row): Boolean = {
    val camCode = row.getAs[String](ColNames.CamCode)
    camCode != null
  }

  def extract(mcoCE: DataFrame, ccamCodes: Seq[String]): Dataset[Event[MedicalAct]] = {
    import mcoCE.sqlContext.implicits._

    mcoCE.select(ColNames.allCols.map(col): _*)
      .filter(correctCamCode(ccamCodes) _)
      .select(outputCols: _*)
      .map(McoCEAct.fromRow(_))
  }
}

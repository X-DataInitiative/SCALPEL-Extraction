package fr.polytechnique.cmap.cnam.etl.extractors

import java.sql.Timestamp

import fr.polytechnique.cmap.cnam.etl.events._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

class McoCEMedicalActEventExtractor extends Serializable with MCOCESourceInfo {
  @transient final lazy val outputCols = List(
    col(MCOCECols.PatientID).as("patientID"),
    col(MCOCECols.CamCode).as("code"),
    col(MCOCECols.Date).cast(TimestampType).as("eventDate"),
    lit("ACE").as("groupID")
  )
  def correctCamCode(camCodes: Seq[String])(row: Row): Boolean = {
    val camCode = row.getAs[String](MCOCECols.CamCode)
    if (camCode != null) camCodes.map(camCode.startsWith).exists(identity) else false
  }
  def extract(mcoCE: DataFrame, ccamCodes: Seq[String]): Dataset[Event[MedicalAct]] = {
    import mcoCE.sqlContext.implicits._
    mcoCE.select(MCOCECols.getColumns.map(col): _*).filter(correctCamCode(ccamCodes) _)
      .select(outputCols: _*).map(McoCEAct.fromRow(_))
  }
}

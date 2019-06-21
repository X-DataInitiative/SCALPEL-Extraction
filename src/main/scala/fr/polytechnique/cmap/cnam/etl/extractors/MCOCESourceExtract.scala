package fr.polytechnique.cmap.cnam.etl.extractors

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.sources.Sources

class McoCEMedicalActEventExtractor(sources: Sources, ccamCodes: Seq[String]) extends SourceExtractor with MCOCESourceInfo {
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
  override def select() : DataFrame = {
    val mcoCE = sources.mcoCe.get
    import mcoCE.sqlContext.implicits._
    mcoCE.select(MCOCECols.getColumns.map(col): _*).filter(correctCamCode(ccamCodes) _)
      .select(outputCols: _*)

  }
  override def extract[A <: AnyEvent]() : Dataset[Event[A]] = {
    val df = select()
    import df.sqlContext.implicits._
    df.map(McoCEAct.fromRow(_)).asInstanceOf[Dataset[Event[A]]]
  }
}

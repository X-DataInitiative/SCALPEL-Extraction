package fr.polytechnique.cmap.cnam.etl.events.acts

import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.etl.events.Event
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions.col
import fr.polytechnique.cmap.cnam.util.McoUtils._

object McoActs {

  object McoColumns extends McoTimeColumns{
    val patientID: Column = col("NUM_ENQ").as("patientID")
    val actCode: Column = col("`MCO_A.CDC_ACT`").as("code")

    override def inputColumns: List[Column] = {
      /*this.getClass.getDeclaredFields.map {
        field =>
          field.setAccessible(true)
          field.get(this).asInstanceOf[Column]
      }.toList*/
      List(patientID, actCode, stayStart, stayEnd, stayEndMonth, stayEndYear, stayLength)
    }

    def code: Column = col("code")

  }

  implicit class ActDF(data: DataFrame) {
    def filterActs(acts: List[String]): DataFrame = {
      if (acts.isEmpty)
        data.sparkSession.emptyDataFrame
      else
        data.where(acts.map(McoColumns.code.startsWith).reduce(_ or _))
    }
  }

  def extract(config: ExtractionConfig, dcir: DataFrame): Dataset[Event[MedicalAct]] = {
    import dcir.sqlContext.implicits._

    val acts = config.dcirActCodes

    dcir.select(McoColumns.inputColumns: _*)
      .estimateStayStartTime
      .filterActs(acts)
      .map(HospitalizedAct.fromRow(_))
  }
}

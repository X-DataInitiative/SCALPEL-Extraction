package fr.polytechnique.cmap.cnam.etl.events.acts

import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.etl.events.Event
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions.col

object McoActs {

  private object McoColumns {
    val patientID: Column = col("NUM_ENQ")
    val stayStart: Column = col("ENT_DAT").cast("Timestamp")
    val stayEnd: Column = col("SOR_DAT").cast("Timestamp")
    val stayEndMonth: Column = col("`MCO_B.SOR_MOI`")
    val stayEndYear: Column = col("`MCO_B.SOR_ANN`")
    val stayLength: Column = col("`MCO_B.SEJ_NBJ`")
    val actCode: Column = col("`MCO_A.CDC_ACT`")
  }

  val inputColumns: List[Column] = {
    import McoColumns._
    List(
      patientID,
      stayStart,
      stayEnd,
      stayEndMonth,
      stayEndYear,
      stayLength,
      actCode
    )
  }

  implicit class ActDF(data: DataFrame) {
    import McoColumns._

    def filterActs(acts: List[String]): DataFrame = {
      data.where(acts.map(actCode.startsWith).reduce(_ or _))
    }
  }

  def extract(config: ExtractionConfig, dcir: DataFrame): Dataset[Event[MedicalAct]] = {
    import dcir.sqlContext.implicits._

    val acts = config.dcirActCodes

    dcir.select(inputColumns: _*)
      .filterActs(acts)
      .map(HospitalizedAct.fromRow(_))
  }
}

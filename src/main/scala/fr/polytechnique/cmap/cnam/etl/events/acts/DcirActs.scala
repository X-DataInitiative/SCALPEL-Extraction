package fr.polytechnique.cmap.cnam.etl.events.acts

import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions.col
import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.etl.events.Event

private[acts] object DcirActs {

  private object DcirColumns {
    private val patientID: Column = col("NUM_ENQ").as("patientID")
    private val actCode: Column = col("`ER_CAM_F.CAM_PRS_IDE`").as("code")
    private val eventDate: Column = col("EXE_SOI_DTD").as("event_date")

    val inputColumns: List[Column] = List(
      patientID,
      actCode,
      eventDate
    )

    val codeColumn: Column = col("code")
  }

  implicit class ActDF(data: DataFrame) {
    import DcirColumns._

    def filterActs(acts: List[String]): DataFrame = {
      data.where(acts.map(codeColumn.startsWith).reduce(_ or _))
    }
  }

  def extract(config: ExtractionConfig, dcir: DataFrame): Dataset[Event[MedicalAct]] = {
    import dcir.sqlContext.implicits._

    val acts = config.dcirActCodes

    dcir.select(DcirColumns.inputColumns: _*)
      .filterActs(acts)
      .map(NonHospitalizedAct.fromRow(_))
  }
}

package fr.polytechnique.cmap.cnam.filtering

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._


/**
  * Created by burq on 09/08/16.
  */
class CancerTransformer extends Transformer[FlatEvent] {
  val EncodingColumn = "MED_NCL_IDT"
  val DiagnoseColumn = "MED_MTF_COD"
  val CancerICD10 = "C67"

  override def transform(sources: Sources): Dataset[FlatEvent] = {

    val irImbR: DataFrame = sources.irImb.get
    val patient: DataFrame = _

    irImbR.filter(col(EncodingColumn) === "ICD10")
      .filter(col(DiagnoseColumn) contains CancerICD10)
      .join(patient, "NUM_ENQ")
      .select(
        col("PatientID").as("patientID"),
        col("Gender").as("gender"),
        col("BirthDate").as("birthDate"),
        lit("disease").as("category"),
        lit("C67").as("eventID"),
        lit(1.00).as("weight"),
        col("IMB_ALD_DTD").as("start")
      )
      .as[FlatEvent]
  }

}

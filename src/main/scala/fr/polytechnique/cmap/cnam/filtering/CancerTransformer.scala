package fr.polytechnique.cmap.cnam.filtering

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Dataset, Column}
import org.apache.spark.sql.functions._


/**
  * Created by burq on 09/08/16.
  */
object CancerTransformer extends Transformer[FlatEvent] {
  val EncodingColumn = "MED_NCL_IDT"
  val DiagnoseColumn = "MED_MTF_COD"
  val CancerCIM10 = "C67"

  val outputColumns: List[Column] = List(
    col("PatientID").as("patientID"),
    col("Gender").as("gender"),
    col("BirthDate").as("birthDate"),
    lit("disease").as("category"),
    lit("C67").as("eventID"),
    lit(1.00).as("weight"),
    col("IMB_ALD_DTD").as("start")
  )

  implicit class CancerDataFrame(df: DataFrame) {

    def extractCancer(): DataFrame = {
      df.filter(col(EncodingColumn) === "CIM10")
        .filter(col(DiagnoseColumn) contains CancerCIM10)
    }

    def joinWithPatient(other: DataFrame): DataFrame = {
      df.withColumnRenamed("NUM_ENQ", "PatientID")
        .join(other, "PatientID")
    }
  }

  override def transform(sources: Sources): Dataset[FlatEvent] = {

    val irImbR: DataFrame = sources.irImb.get
    val patient: DataFrame = ???

    import irImbR.sqlContext.implicits._

    irImbR.extractCancer()
      .joinWithPatient(patient)
      .select(outputColumns:_*)
      .as[FlatEvent]
  }
}

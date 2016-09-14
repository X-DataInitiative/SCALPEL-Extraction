package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, DataFrame, Dataset}

import fr.polytechnique.cmap.cnam.utilities.functions._

object IrBenPatientTransformer extends Transformer[Patient] with PatientsTransformer{

  val inputColumns = List(
    col("NUM_ENQ").as("patientID"),
    col("BEN_SEX_COD"),
    col("BEN_NAI_MOI"),
    col("BEN_NAI_ANN"),
    col("BEN_DCD_DTE")
  )

  val outputColumns = List(
    col("patientID"),
    col("gender"),
    col("birthDate"),
    col("deathDate")
  )


  implicit class TransformerData(data: DataFrame) {

    def getGender: DataFrame = {
      val result = data
        .select(
          col("patientID"),
          col("BEN_SEX_COD").as("gender")
        ).distinct

      val patients = result.select(col("patientID")).distinct()

      if (result.count != patients.count)
        throw new Exception("One or more patients have conflicting SEX CODE in IR_BEN_R")

      result
    }

    def getDeathDate: DataFrame = {
      val cleanData = data.filter(col("BEN_DCD_DTE").isNotNull)

      val distinctPatients = cleanData.select("patientID").distinct
      val distinctPatientsDeathDate = cleanData
        .select("patientID", "BEN_DCD_DTE").distinct

      if (distinctPatients.count != distinctPatientsDeathDate.count)
        throw new Exception("One or more patients have conflicting DEATH DATES in IR_BEN_R")

      val columnsToSelect: List[Column] = List(
        col("patientID"),
        col("BEN_DCD_DTE").cast(TimestampType).as("deathDate")
      )

      distinctPatientsDeathDate.select(columnsToSelect: _*)
    }

    def getBirthDate: DataFrame = {

      val birthDate: Column = computeDateUsingMonthYear(col("BEN_NAI_MOI"), col("BEN_NAI_ANN")).as("birthDate")

      val result = data
        .filter(col("BEN_NAI_MOI").between(MinMonth, MaxMonth) &&
        col("BEN_NAI_ANN").between(MinYear, MaxYear))
        .select(col("patientID"), birthDate)
        .distinct
        .cache
      val patients = result.select(col("patientID")).distinct

      // This check makes sure, patients don't have conflicting birth dates.
      if (result.count != patients.count)
        throw new Exception("One or more patients have conflicting BIRTH DATES in IR_BEN_R")

      result
    }

  }


  override def transform(sources: Sources): Dataset[Patient] = {


    val irBen: DataFrame = sources.irBen.get.select(inputColumns: _*).persist()
    import irBen.sqlContext.implicits._

    val birthDates = irBen
      .getBirthDate

    val deathDates = irBen
      .getDeathDate

    irBen.unpersist()

    irBen.getGender
      .join(deathDates, Seq("patientID"), "left_outer")
      .join(birthDates, Seq("patientID"), "left_outer")
      .select(outputColumns: _*)
      .as[Patient]
  }
}

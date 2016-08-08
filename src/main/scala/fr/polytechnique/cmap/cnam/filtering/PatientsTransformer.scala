package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.filtering.utilities.TransformerHelper

/**
  * Transformer object for patients data
  * Note that all transformers should cache the DataFrames that are going to be used
  */
object PatientsTransformer extends Transformer[Patient] {

  private def estimateBirthDate(timestamp1: Column, timestamp2: Column, birthYear: Column) = {
    unix_timestamp(
      concat(
        month(TransformerHelper.getMeanTimestampColumn(timestamp1, timestamp2)),
        lit("-"),
        birthYear
      ), "MM-yyyy"
    ).cast(TimestampType)
  }

  def transform(sources: Sources): Dataset[Patient] = {
    val dcir: DataFrame = sources.dcir.get.cache()

    // Columns definition:
    val patientIDCol = col("NUM_ENQ")
    val genderCol = col("BEN_SEX_COD")
    val deathDateCol = col("BEN_DCD_DTE")
    val ageCol = col("BEN_AMA_COD")
    val eventDateCol = col("EXE_SOI_DTD")
    val eventMonthCol = month(eventDateCol)
    val eventYearCol = year(eventDateCol)
    val birthYearCol = col("BEN_NAI_ANN")

    // Data quality filters. Should we move to the extractor?
    val minAge = 18L
    val maxAge = 1000L
    val minGender = 1L
    val maxGender = 2L
    val minYear = 1800L
    val maxYear = 2100L

    val patientsWithAge: DataFrame = dcir
      .where(
        ageCol.between(minAge, maxAge) &&
        genderCol.between(minGender, maxGender) &&
        birthYearCol.between(minYear, maxYear) &&
        ( year(deathDateCol).isNull || year(deathDateCol).between(minYear, maxYear) )
      )
      .groupBy(patientIDCol, ageCol)
      .agg(
        sum(lit(1)).as("rowCount"), // We will use it to find the appropriate gender and birth year
        sum(genderCol).as("genderSum"), // We will use it to find the appropriate gender
        sum(birthYearCol).as("birthYearSum"), // We will use it to find the appropriate birth year
        min(eventDateCol).as("minEventDate"), // the min event date for each age of a patient
        max(eventDateCol).as("maxEventDate"), // the max event date for each age of a patient
        min(deathDateCol).cast(TimestampType).as("deathDate") // the earliest death date
      )

    val avgGenderCol = round(sum(col("genderSum")) / sum(col("rowCount"))).cast(IntegerType)
    val avgBirthYearCol = round(sum(col("birthYearSum")) / sum("rowCount")).cast(IntegerType)

    val birthDateAggCol: Column = estimateBirthDate(
      max(col("minEventDate")).cast(TimestampType),
      min(col("maxEventDate")).cast(TimestampType),
      avgBirthYearCol
    )

    import dcir.sqlContext.implicits._

    val patients: Dataset[Patient] = patientsWithAge
      .groupBy(patientIDCol.as("patientID"))
      .agg(
        avgGenderCol.as("gender"),
        birthDateAggCol.as("birthDate"),
        first(col("deathDate")).as("deathDate")
      )
      .as[Patient]

    patients
  }
}

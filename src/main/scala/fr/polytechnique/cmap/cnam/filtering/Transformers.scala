package fr.polytechnique.cmap.cnam.filtering

import fr.polytechnique.cmap.cnam.filtering.utilities.TransformerHelper
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * @author Daniel de Paula
  */
trait Transformer[T] {
  def transform(sources: Sources): Dataset[T]
}

/**
  * Transformer object for patients data
  * Note that all transformers should cache the DataFrames that are going to be used
  *
  * @author Daniel de Paula
  */
object PatientsTransformer extends Transformer[Patient] {

  private def estimateBirthDate(timestamp1: Column, timestamp2: Column) = {
    unix_timestamp(
      concat(
        month(TransformerHelper.getMeanTimestampColumn(timestamp1, timestamp2)),
        lit("-"),
        first(col("birthYear"))
      ), "MM-yyyy"
    ).cast(TimestampType)
  }

  def transform(sources: Sources): Dataset[Patient] = {
    val dcir: DataFrame = sources.dcir.get.cache()

    // Columns definition:
    val patientIDCol = (col("NUM_ENQ"))
    val genderCol = col("BEN_SEX_COD")
    val deathDateCol = col("BEN_DCD_DTE")
    val ageCol = col("BEN_AMA_COD")
    val eventDateCol = col("EXE_SOI_DTD")
    val eventMonthCol = month(eventDateCol)
    val eventYearCol = year(eventDateCol)
    val birthYearCol = col("BEN_NAI_ANN")

    val minAge = 18L
    val maxAge = 1000L

    val patientsWithAge: DataFrame = dcir
      .where(ageCol.between(minAge, maxAge))
      .groupBy(patientIDCol, ageCol)
      .agg(
        first(genderCol).cast(IntegerType).as("gender"), // we take any gender, as they should be the same
        min(eventDateCol).as("minEventDate"), // the min event date for each age of a patient
        max(eventDateCol).as("maxEventDate"), // the max event date for each age of a patient
        min(deathDateCol).cast(TimestampType).as("deathDate"),
        first(birthYearCol).as("birthYear") // we take any birth year, as they should be the same
      )

    val birthDateAggCol: Column = estimateBirthDate(
      max(col("minEventDate")).cast(TimestampType),
      min(col("maxEventDate")).cast(TimestampType)
    )

    import dcir.sqlContext.implicits._

    val patients: Dataset[Patient] = patientsWithAge
      .groupBy(patientIDCol.as("patientID"))
      .agg(
        first(col("gender")).cast(IntegerType).as("gender"),
        birthDateAggCol.as("birthDate"),
        first("deathDate").as("deathDate")
      )
      .as[Patient]

    patients
  }
}

// todo: Implement other transformer objects.
// Note that all transformers should cache the DataFrames that are going to be used */

/*
object DrugEventsTransformer extends Transformer[FlatEvent] {

  def transform(sources: Sources): Dataset[FlatEvent] = {

  }
}
*/

/*
object DiseaseEventsTransformer extends Transformer[FlatEvent] {

  def transform(sources: Sources): Dataset[FlatEvent] = {

  }
}
 */

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

  val minAge = 18
  val maxAge = 1000
  val minGender = 1
  val maxGender = 2
  val minYear = 1900
  val maxYear = 2020

  val dcirCols: List[Column] = List(
    col("NUM_ENQ").as("patientID"),
    when(
      !col("BEN_SEX_COD").between(minGender, maxGender), null
    ).otherwise(col("BEN_SEX_COD")).as("gender"),
    col("BEN_AMA_COD").as("age"),
    col("BEN_NAI_ANN").as("birthYear"),
    col("EXE_SOI_DTD").as("eventDate"),
    month(col("EXE_SOI_DTD")).as("eventMonth"),
    year(col("EXE_SOI_DTD")).as("eventYear"),
    col("BEN_DCD_DTE").as("deathDate")
  )

  def estimateBirthDate(ts1: Column, ts2: Column, birthYear: Column): Column = {
    unix_timestamp(
      concat(
        month(TransformerHelper.getMeanTimestampColumn(ts1, ts2)),
        lit("-"),
        birthYear
      ), "MM-yyyy"
    ).cast(TimestampType)
  }

  def averageGender(genderSum: Column, genderCount: Column): Column = {
    round(genderSum.cast(DoubleType) / genderCount).cast(IntegerType)
  }

  def selectFromDCIR(dcir: DataFrame): DataFrame = {
    dcir.select(dcirCols: _*).where(
      col("age").between(minAge, maxAge) &&
      (col("deathDate").isNull || year(col("deathDate")).between(minYear, maxYear))
    )
  }

  def computeBirthYear(initialEvents: DataFrame): DataFrame = {
    initialEvents
      .groupBy(col("patientID"), col("birthYear")).agg(count("*").as("count"))
      .orderBy(col("patientID"), col("count").desc)
      .groupBy(col("patientID").as("patientID")).agg(first("birthYear").as("birthYear"))
  }

  def groupByIdAndAge(initialEvents: DataFrame): DataFrame = {
    initialEvents
      .groupBy(col("patientID"), col("age"))
      .agg(
        count("gender").as("genderCount"), // We will use it to find the appropriate gender
        sum("gender").as("genderSum"), // We will use it to find the appropriate gender
        min("eventDate").as("minEventDate"), // the min event date for each age of a patient
        max("eventDate").as("maxEventDate"), // the max event date for each age of a patient
        min("deathDate").cast(TimestampType).as("deathDate") // the earliest death date
      )
  }

  def joinWithBirthYear(groupedByIdAndAge: DataFrame, birthYears: DataFrame): DataFrame = {
    groupedByIdAndAge.as("p").join(birthYears.as("y"), "patientID")
  }

  def aggregatePatients(groupedWithBirthYear: DataFrame): Dataset[Patient] = {

    val birthDateAggCol: Column = estimateBirthDate(
      max(col("minEventDate")).cast(TimestampType),
      min(col("maxEventDate")).cast(TimestampType),
      first(col("birthYear"))
    )

    import groupedWithBirthYear.sqlContext.implicits._

    groupedWithBirthYear
      .groupBy(col("patientID"))
      .agg(
        averageGender(sum(col("genderSum")), sum(col("genderCount"))).as("gender"),
        birthDateAggCol.as("birthDate"),
        min(col("deathDate")).as("deathDate")
      )
      .as[Patient]
  }

  def transform(sources: Sources): Dataset[Patient] = {
    val dcir: DataFrame = sources.dcir.get.cache()

    val dcirSelected = selectFromDCIR(dcir)
    val withBirthYear = computeBirthYear(dcirSelected)
    val grouped = groupByIdAndAge(dcirSelected)
    val joined = joinWithBirthYear(grouped, withBirthYear)
    aggregatePatients(joined)
  }
}

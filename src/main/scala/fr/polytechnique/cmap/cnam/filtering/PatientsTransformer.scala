package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import fr.polytechnique.cmap.cnam.filtering.utilities.TransformerHelper

/**
  * Transformer object for patients data
  * Note that all transformers should cache the DataFrames that are going to be used
  */
object PatientsTransformer extends Transformer[Patient] {

  def estimateBirthDateCol(ts1: Column, ts2: Column, birthYear: Column): Column = {
    unix_timestamp(
      concat(
        month(TransformerHelper.getMeanTimestampColumn(ts1, ts2)),
        lit("-"),
        birthYear
      ), "MM-yyyy"
    ).cast(TimestampType)
  }

  implicit class PatientTransformerPipeline(data: DataFrame) {

    final val MinAge = 18
    final val MaxAge = 1000
    final val MinGender = 1
    final val MaxGender = 2
    final val MinYear = 1900
    final val MaxYear = 2020

    def selectFromDCIR: DataFrame = {
      val dcirCols: List[Column] = List(
        col("NUM_ENQ").cast(StringType).as("patientID"),
        when(
          // Invalid values of gender will be set to null in order to allow the "average" gender to
          //   be calculated and then the correct gender to be found
          !col("BEN_SEX_COD").between(MinGender, MaxGender), null
        ).otherwise(col("BEN_SEX_COD")).cast(IntegerType).as("gender"),
        col("BEN_AMA_COD").cast(IntegerType).as("age"),
        col("BEN_NAI_ANN").cast(StringType).as("birthYear"),
        col("EXE_SOI_DTD").cast(DateType).as("eventDate"),
        month(col("EXE_SOI_DTD")).cast(IntegerType).as("eventMonth"),
        year(col("EXE_SOI_DTD")).cast(IntegerType).as("eventYear"),
        col("BEN_DCD_DTE").cast(DateType).as("deathDate")
      )

      data.select(dcirCols: _*).where(
        col("age").between(MinAge, MaxAge) &&
          (col("deathDate").isNull || year(col("deathDate")).between(MinYear, MaxYear))
      )
    }

    // The birth year for each patient is found by grouping by patientId and birthYear and then
    //   by taking the most frequent birth year for each patient.
    def findBirthYears: DataFrame = {
      val window = Window.partitionBy(col("patientID")).orderBy(col("count").desc, col("birthYear"))
      data
        .groupBy(col("patientID"), col("birthYear")).agg(count("*").as("count"))
        // "first" is only deterministic when applied over an ordered window:
        .select(col("patientID"), first(col("birthYear")).over(window).as("birthYear"))
        .distinct
    }

    // After selecting the data, the next step is to group by patientId and age, because we need to
    //   estimate the birthDate ant we use min(eventDate) and max(eventDate) for each age to achieve
    //   that.
    def groupByIdAndAge: DataFrame = {
      data
        .groupBy(col("patientID"), col("age"))
        .agg(
          count("gender").as("genderCount"), // We will use it to find the appropriate gender (avg)
          sum("gender").as("genderSum"), // We will use it to find the appropriate gender (avg)
          min("eventDate").as("minEventDate"), // the min event date for each age of a patient
          max("eventDate").as("maxEventDate"), // the max event date for each age of a patient
          min("deathDate").as("deathDate") // the earliest death date
        )
    }

    // Then we aggregate again by taking the mean between the closest dates where the age changed.
    // For example, if the patient was 60yo when an event happened on Apr/2010 and he was 61yo when
    //   another event happened on Jun/2010, we calculate the mean and estimate his birthday as
    //   being in May of the year found in "findBirthYears"
    def estimateFields: Dataset[Patient] = {
      val birthDateAggCol: Column = estimateBirthDateCol(
        max(col("minEventDate")).cast(TimestampType),
        min(col("maxEventDate")).cast(TimestampType),
        first(col("birthYear"))
      )

      import data.sqlContext.implicits._ // Necessary for the DataFrame -> Dataset conversion
      data
        .groupBy(col("patientID"))
        .agg(
          // Here we calculate the average of gender values and then we round. So, if 1 is more
          //   common, the average will be less than 1.5 and the final value will be 1. The same is
          //   valid for the case where 2 is more common. This is the reason why we set invalid
          //   values for gender to null.
          round(sum(col("genderSum")) / sum(col("genderCount"))).cast(IntegerType).as("gender"),
          birthDateAggCol.as("birthDate"),
          min(col("deathDate")).cast(TimestampType).as("deathDate")
        )
        .as[Patient]
    }
  }

  def transform(sources: Sources): Dataset[Patient] = {
    val dcir: DataFrame = sources.dcir.get.selectFromDCIR.cache()
    val birthYears: DataFrame = dcir.findBirthYears
    dcir
      .groupByIdAndAge
      .join(birthYears, "patientID")
      .estimateFields
  }
}

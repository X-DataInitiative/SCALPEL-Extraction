package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
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
    private final val MinAge = 18
    private final val MaxAge = 1000
    private final val MinGender = 1
    private final val MaxGender = 2
    private final val MinYear = 1900
    private final val MaxYear = 2020

    private final val DcirCols: List[Column] = List(
      col("NUM_ENQ").as("patientID"),
      when(
        !col("BEN_SEX_COD").between(MinGender, MaxGender), null
      ).otherwise(col("BEN_SEX_COD")).as("gender"),
      col("BEN_AMA_COD").as("age"),
      col("BEN_NAI_ANN").as("birthYear"),
      col("EXE_SOI_DTD").as("eventDate"),
      month(col("EXE_SOI_DTD")).as("eventMonth"),
      year(col("EXE_SOI_DTD")).as("eventYear"),
      col("BEN_DCD_DTE").as("deathDate")
    )

    def selectFromDCIR: DataFrame = {
      data.select(DcirCols: _*).where(
        col("age").between(MinAge, MaxAge) &&
          (col("deathDate").isNull || year(col("deathDate")).between(MinYear, MaxYear))
      )
    }

    def findBirthYears: DataFrame = {
      data
        .groupBy(col("patientID"), col("birthYear")).agg(count("*").as("count"))
        .orderBy(col("patientID"), col("count").desc)
        .groupBy(col("patientID").as("patientID")).agg(first("birthYear").as("birthYear"))
    }

    def groupByIdAndAge: DataFrame = {
      data
        .groupBy(col("patientID"), col("age"))
        .agg(
          count("gender").as("genderCount"), // We will use it to find the appropriate gender
          sum("gender").as("genderSum"), // We will use it to find the appropriate gender
          min("eventDate").as("minEventDate"), // the min event date for each age of a patient
          max("eventDate").as("maxEventDate"), // the max event date for each age of a patient
          min("deathDate").cast(TimestampType).as("deathDate") // the earliest death date
        )
    }

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
          round(sum(col("genderSum")) / sum(col("genderCount"))).cast(IntegerType).as("gender"),
          birthDateAggCol.as("birthDate"),
          min(col("deathDate")).as("deathDate")
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

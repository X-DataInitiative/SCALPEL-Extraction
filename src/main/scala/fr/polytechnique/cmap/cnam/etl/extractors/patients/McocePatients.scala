package fr.polytechnique.cmap.cnam.etl.extractors.patients

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.extractors.patients.PatientUtils.estimateBirthDateCol
import fr.polytechnique.cmap.cnam.etl.patients.Patient

private[patients] object McocePatients {

  implicit class McocePatientsImplicit(mce: DataFrame) {

    def calculateBirthYear: DataFrame = {
      val win = Window.partitionBy("patientID")

      val birthYear = min(col("event_year") - col("age"))
        .over(win)
        .as("birth_year")

      mce.groupBy("patientID", "age")
        .agg(max(year(col("event_date"))).as("event_year"))
        .select(col("patientID"), birthYear)
        .distinct
    }

    def groupByIdAndAge: DataFrame = {
      mce.groupBy("patientID", "age")
        .agg(
          sum("sex").cast(DoubleType).as("sum_sex"),
          count("sex").cast(DoubleType).as("count_sex"),
          min("event_date").as("min_event_date"),
          max("event_date").as("max_event_date")
        )
    }

    def calculateBirthDateAndGender: DataFrame = {
      val genderCol = round(sum("sum_sex") / sum("count_sex"))
        .cast(IntegerType)
        .as("gender")

      val birthDateCol = estimateBirthDateCol(max("min_event_date"), min("max_event_date"),
        first("birth_year")).as("birthDate")

      mce.groupBy("patientID")
        .agg(
          genderCol,
          birthDateCol
        )
    }
  }

  def extract(
    mcoce: DataFrame,
    minGender: Int,
    maxGender: Int,
    minYear: Int,
    maxYear: Int): Dataset[Patient] = {

    val sexCol = when(col("MCO_FASTC__COD_SEX").cast(IntegerType)
      .between(minGender, maxGender), col("MCO_FASTC__COD_SEX"))
      .cast(IntegerType)
      .as("sex")

    val eventDateCol = when(year(col("EXE_SOI_DTD"))
      .between(minYear, maxYear), col("EXE_SOI_DTD"))
      .cast(TimestampType)
      .as("event_date")

    val ageCol = col("MCO_FASTC__AGE_ANN")
      .cast(IntegerType)
      .as("age")

    val inputCols = List(
      col("NUM_ENQ").as("patientID"),
      sexCol,
      ageCol,
      eventDateCol
    )
    val mcoceFiltered = mcoce.select(inputCols: _*)

    val birthYears = mcoceFiltered.calculateBirthYear

    import mcoce.sparkSession.implicits._
    mcoceFiltered.groupByIdAndAge
      .join(birthYears, "patientID")
      .calculateBirthDateAndGender
      .withColumn("deathDate", lit(null).cast(TimestampType))
      .as[Patient]
  }

}

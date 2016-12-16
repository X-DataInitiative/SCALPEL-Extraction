package fr.polytechnique.cmap.cnam.filtering.cox

import java.sql.Timestamp
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.filtering.{DatasetTransformer, FilteringConfig, FlatEvent}

// Start and End are expressed in month from the patient startObs
case class CoxFeature(
  patientID: String,
  gender: Int,
  age: Int,
  ageGroup: String,
  start: Int,
  end: Int,
  hasCancer: Int = 0,
  insuline: Int = 0,
  sulfonylurea: Int = 0,
  metformine: Int = 0,
  pioglitazone: Int = 0,
  rosiglitazone: Int = 0,
  other: Int = 0
)

object CoxTransformer extends DatasetTransformer[FlatEvent, CoxFeature] {

  val moleculesList = List(
    "insuline",
    "sulfonylurea",
    "metformine",
    "pioglitazone",
    "rosiglitazone",
    "other"
  )

  final val AgeReferenceDate: Timestamp = FilteringConfig.dates.ageReference

  implicit class CoxDataFrame(data: DataFrame) {

    def withHasCancer: DataFrame = {
      data.withColumn("hasCancer", when(col("endReason") === "disease", 1).otherwise(0))
    }

    def withAge: DataFrame = {
      data.withColumn("age",
        (months_between(lit(AgeReferenceDate), col("birthDate"))).cast(IntegerType)
      )
    }

    def withAgeGroup: DataFrame = {
      val ageStep = 5
      val groupMin = floor(col("age") / (12.0 * ageStep)) * ageStep
      val ageGroup = concat(groupMin, lit("-"), groupMin + ageStep - 1)
      data.withColumn("ageGroup", ageGroup)
    }

    def normalizeDates: DataFrame = {
      def normalize(c: Column) = months_between(c, col("followUpStart")).cast(IntegerType)

      data
        .withColumn("start", normalize(col("start")))
        .withColumn("end", normalize(col("end")))
    }

    def stackDates: DataFrame = {
      data.withColumn("coxStart", col("start"))
        .unionAll(data.withColumn("coxStart", col("end")))
    }

    def withCoxEnd: DataFrame = {
      val window = Window.partitionBy("patientID").orderBy("coxStart")
      data
        .withColumn("coxEnd", lead(col("coxStart"), 1).over(window))
        .withColumn("coxEnd",
          when(col("coxEnd") > col("coxStart"), col("coxEnd"))
        )
    }

    def prepareToPivot(rawExposures: DataFrame): DataFrame = {
      val toJoin = rawExposures.select(
        col("patientID").as("exp_patientID"),
        col("moleculeName").as("exp_moleculeName"),
        col("start").as("exp_start"),
        col("end").as("exp_end")
      )

      data
        .join(toJoin,
          col("exp_patientID") === col("patientID") &&
          (col("exp_start") <= col("coxStart") &&
          (col("exp_end") >= col("coxEnd")))
        )
        .withColumn("moleculeName", col("exp_moleculeName"))
    }

    def pivotMolecules: DataFrame = {
      data
        .withColumn("moleculeName", lower(col("moleculeName")))
        .groupBy(
          col("patientID"),
          col("gender"),
          col("age"),
          col("ageGroup"),
          col("coxStart").as("start"),
          col("coxEnd").as("end"),
          col("hasCancer")
        )
        .pivot("moleculeName", moleculesList).agg(count("moleculeName").cast(IntegerType)).persist
    }

    def adjustCancerValues: DataFrame = {
      val window = Window.partitionBy("patientID").orderBy(col("end").desc, col("start").desc)
      data
        .withColumn("rank", row_number().over(window))
        .withColumn("hasCancer", when(col("rank") === 1, col("hasCancer")).otherwise(0))
        .drop("rank")
    }
  }

  def transform(events: Dataset[FlatEvent]): Dataset[CoxFeature] = {
    import CoxFollowUpEventsTransformer.FollowUpFunctions
    import events.sqlContext.implicits._

    val exposures = events.toDF.repartition(col("patientID"))
      .withFollowUpPeriodFromEvents
      .withEndReasonFromEvents
      .withHasCancer
      .withAge
      .withAgeGroup
      .where(col("category") === "exposure")
      .withColumnRenamed("eventId", "moleculeName")
      .normalizeDates

    exposures
      .stackDates
      .withCoxEnd
      .na.drop("any", Seq("coxStart", "coxEnd"))
      .prepareToPivot(exposures)
      .pivotMolecules
      .adjustCancerValues
      .where(col("start") >= 0) //Avoids negative start (exposures before followUpStart) in cumulative exposure
      .as[CoxFeature]
  }
}

package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

// Start and End are expressed in month from the patient startObs
case class CoxFeature(
  patientID: String,
  gender: Int,
  age: Int,
  start: Int,
  end: Int,
  hasCancer: Int = 0,
  insuline: Int = 0,
  sulfonylurea: Int = 0,
  metformine: Int = 0,
  pioglitazone: Int = 0,
  rosiglitazone: Int = 0,
  other: Int = 0,
  age40_44: Int,
  age45_49: Int,
  age50_54: Int,
  age55_59: Int,
  age60_64: Int,
  age65_69: Int,
  age70_74: Int,
  age75_79: Int
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

  implicit class CoxDataFrame(data: DataFrame) {

    def withHasCancer: DataFrame = {
      data.withColumn("hasCancer", when(col("endReason") === "disease", 1).otherwise(0))
    }

    def withAge: DataFrame = {
      data.withColumn("age",
        (months_between(col("followUpStart"), col("birthDate")) - 6.0).cast(IntegerType)
      )
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
          col("coxStart").as("start"),
          col("coxEnd").as("end"),
          col("hasCancer")
        )
        .pivot("moleculeName", moleculesList).agg(count("moleculeName").cast(IntegerType)).persist
    }

    def withAgeGroups: DataFrame = {
      // Note: if any changes are made to the age groups created in this function, the CoxFeature
      //   case class must be changed as well.
      val ageStep = 5
      val firstAge = 40
      val numGroups = 8

      def groupMax(groupMin: Int) = groupMin + ageStep - 1
      def colName(groupMin: Int) = s"age${groupMin}_${groupMax(groupMin)}"
      def isAgeInGroup(groupMin: Int) = when(
        floor(col("age") / 12).between(groupMin, groupMax(groupMin)), 1
      ).otherwise(0)

      (0 until numGroups).map(_ * ageStep + firstAge).foldLeft(data) {
        (df: DataFrame, groupMin: Int) => df.withColumn(colName(groupMin), isAgeInGroup(groupMin))
      }
    }

    def adjustCancerValues: DataFrame = {
      val window = Window.partitionBy("patientID").orderBy(col("end").desc)
      data
        .withColumn("rank", row_number().over(window))
        .withColumn("hasCancer", when(col("rank") === 1, col("hasCancer")).otherwise(0))
        .drop("rank")
    }
  }

  def transform(events: Dataset[FlatEvent]): Dataset[CoxFeature] = {
    import FollowUpEventsTransformer.FollowUpFunctions
    import events.sqlContext.implicits._

    val exposures = events.toDF.repartition(col("patientID"))
      .withFollowUpPeriodFromEvents
      .withEndReasonFromEvents
      .withHasCancer
      .withAge
      .where(col("category") === "exposure")
      .withColumnRenamed("eventId", "moleculeName")
      .normalizeDates

    exposures
      .stackDates
      .withCoxEnd
      .na.drop("any", Seq("coxStart", "coxEnd"))
      .prepareToPivot(exposures)
      .pivotMolecules
      .withAgeGroups
      .adjustCancerValues
      .as[CoxFeature]
  }
}

package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.utilities.functions._

object ExposuresTransformer extends DatasetTransformer[FlatEvent, FlatEvent] {

  // Constant definitions for delays and time windows. Should be verified before compiling.
  // In the future, we may want to export them to an external file.
  final val ExposureStartDelay = 3
  final val ExposureStartInterval = 6
  final val PeriodStart = makeTS(2006, 1, 1)
  final val DiseaseCode = "C67"

  val outputColumns = List(
    col("patientID"),
    col("gender"),
    col("birthDate"),
    col("deathDate"),
    lit("exposure").as("category"),
    col("eventId"),
    lit(1.0).as("weight"),
    col("exposureStart").as("start"),
    col("followUpEnd").as("end")
  )

  implicit class ExposuresDataFrame(data: DataFrame) {

    def withFollowUpPeriod: DataFrame = {
      val window = Window.partitionBy("patientID")

      val followUpStart: Column = when(col("category") === "followUpPeriod", col("start"))
      val followUpEnd: Column = when(col("category") === "followUpPeriod", col("end"))

      data
        .withColumn("followUpStart", min(followUpStart).over(window))
        .withColumn("followUpEnd", min(followUpEnd).over(window))
    }

    def filterPatients: DataFrame = {
      val window = Window.partitionBy("patientID")

      // Drop patients whose got a target disease before the start of the follow up
      val diseaseFilter = min(
        when(
          col("category") === "disease" && (col("start") < col("followUpStart")),
        lit(0)).otherwise(lit(1))
      ).over(window).cast(BooleanType)

      // Drop patients whose first molecule event is after startPeriod + 1 year
      val firstYearObservation = add_months(lit(PeriodStart), 12).cast(TimestampType)
      val drugFilter = max(
        when(
          col("category") === "molecule" && (col("start") <= firstYearObservation),
          lit(1)
        ).otherwise(lit(0))
      ).over(window).cast(BooleanType)

      data.withColumn("diseaseFilter", diseaseFilter)
        .withColumn("drugFilter", drugFilter)
        .where(col("diseaseFilter") && col("drugFilter"))
    }

    // Ideally, this method must receive only molecules events, otherwise they will treat diseases
    //   as molecules and add an exposure start date for them.
    // The exposure start date will be null when the patient was not exposed.
    def withExposureStart: DataFrame = {
      val window = Window.partitionBy("patientID", "eventId")

      val exposureStartRule: Column = when(
        months_between(col("previousStartDate"), col("start")) <= ExposureStartInterval,
          add_months(col("start"), ExposureStartDelay).cast(TimestampType)
      )

      data
        .withColumn("previousStartDate", lag(col("start"), 1).over(window.orderBy("start")))
        .withColumn("exposureStart", exposureStartRule)
        .withColumn("exposureStart", when(col("exposureStart") < col("followUpStart"),
          col("followUpStart")).otherwise(col("exposureStart"))
        )
        .withColumn("exposureStart", min("exposureStart").over(window))
    }
  }

  def transform(input: Dataset[FlatEvent]): Dataset[FlatEvent] = {
    import input.sqlContext.implicits._

    val events = input.toDF.repartition(col("patientID"))
    events
      .withFollowUpPeriod
      .filterPatients
      .where(col("category") === "molecule")
      .where(col("start") < col("followUpEnd"))
      .withExposureStart
      .where(col("exposureStart").isNotNull)
      .select(outputColumns: _*)
      .dropDuplicates(Seq("patientID", "eventId"))
      .as[FlatEvent]
  }
}

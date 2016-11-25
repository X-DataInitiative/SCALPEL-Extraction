package fr.polytechnique.cmap.cnam.filtering.cox

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.filtering.cox.CoxConfig.CoxExposureDefinition
import fr.polytechnique.cmap.cnam.filtering.{ExposuresTransformer, FilteringConfig, FlatEvent}

object CoxExposuresTransformer extends ExposuresTransformer {

  // Constant definitions for delays and time windows. Should be verified before compiling.
  // In the future, we may want to export them to an external file.
  final val ExposureDefinition: CoxExposureDefinition = CoxConfig.exposureDefinition
  final val DelayedEntriesThreshold: Int = CoxConfig.delayedEntriesThreshold
  final val DiseaseCode: String = FilteringConfig.diseaseCode

  val outputColumns = List(
    col("patientID"),
    col("gender"),
    col("birthDate"),
    col("deathDate"),
    lit("exposure").as("category"),
    col("eventId"),
    lit(1.0).as("weight"),
    col("exposureStart").as("start"),
    col("exposureEnd").as("end")
  )

  implicit class ExposuresDataFrame(data: DataFrame) {

    def filterPatients(filterDelayedPatients: Boolean): DataFrame = {
      val window = Window.partitionBy("patientID")

      // Drop patients who got the disease before the start of the follow up
      val diseaseFilter = min(
        when(
          col("category") === "disease" &&
            col("eventId") === DiseaseCode &&
            col("start") < col("followUpStart"), lit(0))
          .otherwise(lit(1))
      ).over(window).cast(BooleanType)

      // Drop patients whose first molecule event is after PeriodStart + 1 year
      val firstYearObservation = add_months(
        lit(StudyStart),
        DelayedEntriesThreshold
      ).cast(TimestampType)
      val drugFilter = max(
        when(
          col("category") === "molecule" && (col("start") <= firstYearObservation),
          lit(1)
        ).otherwise(lit(0))
      ).over(window).cast(BooleanType)

      val diseaseFilteredPatients = data
        .withColumn("diseaseFilter", diseaseFilter)
        .where(col("diseaseFilter"))

      if (filterDelayedPatients)
        diseaseFilteredPatients
          .withColumn("drugFilter", drugFilter)
          .where(col("drugFilter"))
      else
        diseaseFilteredPatients
    }

    // Ideally, this method must receive only molecules events, otherwise they will treat diseases
    //   as molecules and add an exposure start date for them.
    // The exposure start date will be null when the patient was not exposed.
    def withExposureStart(exposureDefinition: CoxExposureDefinition): DataFrame = {
      val window = Window.partitionBy("patientID", "eventId")

      val exposureStartRule: Column = when(
        months_between(col("start"), col("previousStartDate")) <= exposureDefinition.purchasesWindow,
          add_months(col("start"), exposureDefinition.startDelay).cast(TimestampType)
      )

      val potentialExposureStart: Column = if(exposureDefinition.minPurchases == 1)
        col("start")
      else
        lag(col("start"), exposureDefinition.minPurchases - 1).over(window.orderBy("start"))

      data
        .withColumn("previousStartDate", potentialExposureStart)
        .withColumn("exposureStart", exposureStartRule)
        .withColumn("exposureStart", when(col("exposureStart") < col("followUpStart"),
          col("followUpStart")).otherwise(col("exposureStart"))
        )
        .withColumn("exposureStart", min("exposureStart").over(window))
    }

    def withExposureEnd: DataFrame = data.withColumn("exposureEnd", col("followUpEnd"))
  }

  def transform(input: Dataset[FlatEvent], filterDelayedPatients: Boolean): Dataset[FlatEvent] = {
    import CoxFollowUpEventsTransformer.FollowUpFunctions
    import input.sqlContext.implicits._

    val events = input.toDF.repartition(col("patientID"))
    events
      .withFollowUpPeriodFromEvents
      .filterPatients(filterDelayedPatients)
      .where(col("category") === "molecule")
      .where(col("start") < col("followUpEnd"))
      .withExposureStart(ExposureDefinition)
      .withExposureEnd
      .where(col("exposureEnd") > col("exposureStart"))
      .select(outputColumns: _*)
      .dropDuplicates(Seq("patientID", "eventId"))
      .as[FlatEvent]
  }

  override def transform(input: Dataset[FlatEvent]): Dataset[FlatEvent] = {
    transform(input, filterDelayedPatients = true)
  }
}

package fr.polytechnique.cmap.cnam.filtering.cox

import java.util.Calendar
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.filtering.cox.CoxConfig.CoxExposureDefinition
import fr.polytechnique.cmap.cnam.filtering.{ExposuresTransformer, FilteringConfig, FlatEvent}
import fr.polytechnique.cmap.cnam.utilities.functions._

object CoxExposuresTransformer extends ExposuresTransformer {

  // Constant definitions for delays and time windows. Should be verified before compiling.
  // In the future, we may want to export them to an external file.
  final val DelayedEntriesThreshold: Int = CoxConfig.delayedEntriesThreshold
  final val DiseaseCode: String = FilteringConfig.diseaseCode

  val outputColumns = List(
    col("patientID"),
    col("gender"),
    col("birthDate"),
    col("deathDate"),
    lit("exposure").as("category"),
    col("eventId"),
    col("weight"),
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

    def withExposureStart(exposureDefinition: CoxExposureDefinition): DataFrame = {

      import CoxConfig.ExposureType

      exposureDefinition.cumulativeExposureType match {
        case ExposureType.Simple => withPeriodicExposureStart(exposureDefinition)
        case ExposureType.PurchaseBasedCumulative =>
          withPurchaseBasedCumulativeExposure(exposureDefinition.cumulativeExposureWindow)
        case ExposureType.TimeBasedCumulative => withTimeBasedCumulativeExposure(6, 4)
      }
    }

    // Ideally, this method must receive only molecules events, otherwise they will treat diseases
    //   as molecules and add an exposure start date for them.
    // The exposure start date will be null when the patient was not exposed.
    def withPeriodicExposureStart(exposureDefinition: CoxExposureDefinition): DataFrame = {
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
        .withColumn("weight", lit(1.0))
    }

    def withPurchaseBasedCumulativeExposure(cumulativePeriod: Int): DataFrame = {
      val window = Window.partitionBy("patientID", "eventId")
      val windowCumulativeExposure = window.partitionBy("patientID", "eventId", "exposureStart")

      def normalizedExposureMonth(start: Column) = {
        floor(months_between(start, lit(StudyStart)) / cumulativePeriod).cast(IntegerType)
      }

      val normalizedExposureDate = udf(
        (normalizedMonth: Int) => {
          val cal: Calendar = Calendar.getInstance()
          cal.setTime(StudyStart)
          cal.add(Calendar.MONTH, normalizedMonth * cumulativePeriod)
          makeTS(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1, 1)
        }
      )

      data
        .withColumn("normalizedMonth", normalizedExposureMonth(col("start")))
        .withColumn("exposureStart", normalizedExposureDate(col("normalizedMonth")))
        .withColumn("weight", row_number().over(window.orderBy("start")))
        .withColumn("weight", max("weight").over(windowCumulativeExposure).cast(DoubleType))
    }

    def withTimeBasedCumulativeExposure(exposureStartThreshold: Int,
                                          exposureEndThreshold: Int): DataFrame = {
      val window = Window.partitionBy("patientID", "eventId").orderBy("start")
      import data.sqlContext.implicits._

      val patientsFollowUpEnd = data.select(
        col("patientID").as("data_patientID"),
        col("followUpEnd")
      ).distinct

      // In time-based we share same featuring logic as LTSCCS.
      //TODO: Move LTSCCSExposuresTransformer outside of the LTSCCS package and make it common for both models
      import fr.polytechnique.cmap.cnam.filtering.ltsccs.LTSCCSExposuresTransformer
      val exposures = new LTSCCSExposuresTransformer(exposureStartThreshold, exposureEndThreshold)
        .transform(data.as[FlatEvent])
        .toDF

      val exposuresWithFollowUpEnd = exposures.join(
        patientsFollowUpEnd, col("patientID") === col("data_patientID"), "left_outer")

      exposuresWithFollowUpEnd
        .withColumn("weight", months_between(col("end"), col("start")))
        .withColumn("weight", sum(col("weight")).over(window))
        .withColumn("exposureStart", col("start"))
    }

    def withExposureEnd: DataFrame = data.withColumn("exposureEnd", col("followUpEnd"))
  }

  def transform(input: Dataset[FlatEvent],
      filterDelayedPatients: Boolean,
      exposureDefinition: CoxExposureDefinition): Dataset[FlatEvent] = {

    import CoxFollowUpEventsTransformer.FollowUpFunctions
    import input.sqlContext.implicits._

    val events = input.toDF.repartition(col("patientID"))
    events
      .withFollowUpPeriodFromEvents
      .filterPatients(filterDelayedPatients)
      .where(col("category") === "molecule")
      .where(col("start") < col("followUpEnd"))
      .withExposureStart(exposureDefinition)
      .withExposureEnd
      .where(col("exposureEnd") > col("exposureStart"))
      .dropDuplicates(Seq("patientID", "eventId", "exposureStart", "exposureEnd"))
      .select(outputColumns: _*)
      .as[FlatEvent]
  }

  override def transform(input: Dataset[FlatEvent]): Dataset[FlatEvent] = {
    transform(input, CoxConfig.filterDelayedPatients, CoxConfig.exposureDefinition)
  }
}

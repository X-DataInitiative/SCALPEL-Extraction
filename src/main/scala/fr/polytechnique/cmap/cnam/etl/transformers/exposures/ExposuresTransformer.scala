// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import me.danielpes.spark.datetime.Period
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.etl.events.{Dispensation, Event, Exposure, FollowUp}
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.util.RichDataFrame._

class ExposuresTransformer(config: ExposuresTransformerConfig)
  extends ExposurePeriodAdder with WeightAggregator {

  import Columns._

  val minPurchases: Int = config.minPurchases
  val startDelay: Period = config.startDelay
  val purchasesWindow: Period = config.purchasesWindow

  val exposurePeriodStrategy: ExposurePeriodStrategy = config.periodStrategy
  val endDelay: Option[Period] = config.endDelay
  val endThresholdGc: Option[Period] = config.endThresholdGc
  val endThresholdNgc: Option[Period] = config.endThresholdNgc

  val weightAggStrategy: WeightAggStrategy = config.weightAggStrategy
  val cumulativeExposureWindow: Option[Int] = config.cumulativeExposureWindow
  val cumulativeStartThreshold: Option[Int] = config.cumulativeStartThreshold
  val cumulativeEndThreshold: Option[Int] = config.cumulativeEndThreshold
  val dosageLevelIntervals: Option[List[Int]] = config.dosageLevelIntervals
  val purchaseIntervals: Option[List[Int]] = config.purchaseIntervals

  def transform[Disp <: Dispensation](
    patients: Dataset[(Patient, Event[FollowUp])],
    dispensations: Dataset[Event[Disp]])
  : Dataset[Event[Exposure]] = {

    val inputCols = Seq(
      col("Patient.patientID").as(PatientID),
      col("Patient.gender").as(Gender),
      col("Patient.birthDate").as(BirthDate),
      col("Patient.deathDate").as(DeathDate),
      col("Event.start").as(FollowUpStart),
      col("Event.end").as(FollowUpEnd)
    )

    val input = renameTupleColumns(patients).select(inputCols: _*).join(dispensations, Seq(PatientID))

    import input.sqlContext.implicits._

    input.toDF
      .where(col(Start) <= col(FollowUpEnd)) // This ignores all purchases after the followup End
      .withStartEnd(minPurchases, startDelay, purchasesWindow, endThresholdGc, endThresholdNgc, endDelay)
      .where(col(ExposureStart) =!= col(ExposureEnd)) // This also removes rows where exposureStart = null
      .withColumn(
      "Correct_Exposure_End",
      when(col(FollowUpEnd) < col(ExposureEnd), col(FollowUpEnd)).otherwise(col(ExposureEnd))
      ).drop(ExposureEnd) // This makes sure that all the exposures end at the followup end date
      .withColumnRenamed("Correct_Exposure_End", ExposureEnd)
      .where(col(ExposureStart) <= col(ExposureEnd))
      .aggregateWeight(
        cumulativeExposureWindow,
        cumulativeStartThreshold,
        cumulativeEndThreshold,
        dosageLevelIntervals,
        purchaseIntervals
      )
      .map(
        Exposure.fromRow(
          _,
          nameCol = Value,
          startCol = ExposureStart,
          endCol = ExposureEnd
        )
      )
      .distinct()
  }
}

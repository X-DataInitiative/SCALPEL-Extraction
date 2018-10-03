package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import me.danielpes.spark.datetime.Period
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.etl.events.{Dispensation, Event, Exposure}
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up.FollowUp
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
    patients: Dataset[(Patient, FollowUp)],
    dispensations: Dataset[Event[Disp]])
  : Dataset[Event[Exposure]] = {

    val inputCols = Seq(
      col("Patient.patientID").as(PatientID),
      col("Patient.gender").as(Gender),
      col("Patient.birthDate").as(BirthDate),
      col("Patient.deathDate").as(DeathDate),
      col("FollowUp.start").as(FollowUpStart),
      col("FollowUp.stop").as(FollowUpEnd)
    )

    val input = renameTupleColumns(patients).select(inputCols: _*).join(dispensations, Seq(PatientID))

    import input.sqlContext.implicits._

    input.toDF
      .where(col(Start) <= col(FollowUpEnd)) // This ignores all purchases aftet the followup End
      .withStartEnd(minPurchases, startDelay, purchasesWindow, endThresholdGc, endDelay, endThresholdNgc)
      .where(col(ExposureStart) =!= col(ExposureEnd)) // This also removes rows where exposureStart = null
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

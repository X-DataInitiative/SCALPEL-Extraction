package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import java.sql.Timestamp

import fr.polytechnique.cmap.cnam.etl.events.{Dispensation, Event, Exposure}
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up.FollowUp
import fr.polytechnique.cmap.cnam.util.RichDataFrames._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import me.danielpes.spark.datetime.Period
import me.danielpes.spark.datetime.implicits._

class ExposuresTransformer(config: ExposureDefinition)
  extends ExposurePeriodAdder with WeightAggregator {

  import Columns._

  val exposurePeriodStrategy: ExposurePeriodStrategy = config.periodStrategy
  val weightAggStrategy: WeightAggStrategy = config.weightAggStrategy
  val studyStart: Timestamp = config.studyStart
  val diseaseCode: String = config.diseaseCode
  val filterDelayedPatients: Boolean = config.filterDelayedPatients
  val minPurchases: Int = config.minPurchases
  val startDelay: Period = config.startDelay
  val endDelay: Period = config.endDelay
  val purchasesWindow: Period = config.purchasesWindow
  val tracklossThreshold: Period = config.tracklossThreshold
  val cumulativeExposureWindow: Int = config.cumulativeExposureWindow
  val cumulativeStartThreshold: Int = config.cumulativeStartThreshold
  val cumulativeEndThreshold: Int = config.cumulativeEndThreshold
  val dosageLevelIntervals: List[Int] = config.dosageLevelIntervals
  val purchaseIntervals: List[Int] = config.purchaseIntervals

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

    val input = renameTupleColumns(patients).select(inputCols:_*).join(dispensations, Seq(PatientID))

    import input.sqlContext.implicits._

    input.toDF
      .withStartEnd(minPurchases, startDelay, endDelay, purchasesWindow, tracklossThreshold)
      .where(col(ExposureStart) =!= col(ExposureEnd)) // This also removes rows where exposureStart = null
      .aggregateWeight(
        Some(studyStart),
        Some(cumulativeExposureWindow),
        Some(cumulativeStartThreshold),
        Some(cumulativeEndThreshold),
        Some(dosageLevelIntervals),
        Some(purchaseIntervals)
      )
      .map(Exposure.fromRow(
        _,
        nameCol = Value,
        startCol = ExposureStart,
        endCol = ExposureEnd))
      .distinct()
  }
}

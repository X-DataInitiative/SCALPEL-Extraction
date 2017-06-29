package fr.polytechnique.cmap.cnam.etl.transformer.exposure

import java.sql.Timestamp

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

import fr.polytechnique.cmap.cnam.etl.events.Event
import fr.polytechnique.cmap.cnam.etl.events.exposures.Exposure
import fr.polytechnique.cmap.cnam.etl.events.molecules.Molecule
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.transformer.follow_up.FollowUp
import fr.polytechnique.cmap.cnam.util.RichDataFrames._


class ExposuresTransformer(config: ExposureDefinition)
  extends ExposurePeriodAdder with WeightAggregator with PatientFilters {

  import Columns._

  val exposurePeriodStrategy: ExposurePeriodStrategy = config.periodStrategy
  val weightAggStrategy: WeightAggStrategy = config.weightAggStrategy
  val studyStart: Timestamp = config.studyStart
  val diseaseCode: String = config.diseaseCode
  val filterDelayedPatients: Boolean = config.filterDelayedPatients
  val minPurchases: Int = config.minPurchases
  val startDelay: Int = config.startDelay
  val purchasesWindow: Int = config.purchasesWindow
  val cumulativeExposureWindow: Int = config.cumulativeExposureWindow
  val cumulativeStartThreshold: Int = config.cumulativeStartThreshold
  val cumulativeEndThreshold: Int = config.cumulativeEndThreshold
  val dosageLevelIntervals: List[Int] = config.dosageLevelIntervals
  val purchaseIntervals: List[Int] = config.purchaseIntervals

  def transform(
      patients: Dataset[(Patient, FollowUp)],
      dispensations: Dataset[Event[Molecule]])
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
      .filterPatients(studyStart, diseaseCode, filterDelayedPatients) // TODO: remove
      .where(col(Category) === Molecule.category)
      .withStartEnd(minPurchases, startDelay, purchasesWindow)
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

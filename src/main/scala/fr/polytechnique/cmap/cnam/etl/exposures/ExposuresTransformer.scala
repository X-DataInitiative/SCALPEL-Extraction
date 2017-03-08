package fr.polytechnique.cmap.cnam.etl.exposures

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.etl.old_root.{DatasetTransformer, FlatEvent}
import fr.polytechnique.cmap.cnam.featuring.cox.CoxFollowUpEventsTransformer

class ExposuresTransformer(config: ExposuresConfig)
    extends DatasetTransformer[FlatEvent, FlatEvent]
    // Todo: remove PatientFilters from the ExposureTransformer pipeline (it should be a separate module)
    with PatientFilters
    with ExposurePeriodAdder
    with WeightAggregator {

  lazy val exposurePeriodStrategy: ExposurePeriodStrategy = config.periodStrategy
  lazy val weightAggStrategy: WeightAggStrategy = config.weightAggStrategy

  def transform(input: Dataset[FlatEvent]): Dataset[FlatEvent] = {
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

    import CoxFollowUpEventsTransformer.FollowUpFunctions
    import input.sqlContext.implicits._

    input.toDF
      .withFollowUpPeriodFromEvents
      .filterPatients(config.studyStart, config.diseaseCode, config.filterDelayedPatients)
      .where(col("category") === "molecule")
      .withStartEnd(config.minPurchases,config.startDelay,config.purchasesWindow)
      .where(col("exposureStart") !== col("exposureEnd")) // This also removes rows where exposureStart = null
      .aggregateWeight(
        Some(config.studyStart),
        Some(config.cumulativeExposureWindow),
        Some(config.cumulativeStartThreshold),
        Some(config.cumulativeEndThreshold),
        Some(config.dosageLevelIntervals),
        Some(config.purchaseIntervals)
      )
      .select(outputColumns: _*)
      .dropDuplicates(Seq("patientID", "eventID", "start", "end", "weight"))
      .as[FlatEvent]
  }
}

object ExposuresTransformer {
  def apply(config: ExposuresConfig): ExposuresTransformer = new ExposuresTransformer(config)
}

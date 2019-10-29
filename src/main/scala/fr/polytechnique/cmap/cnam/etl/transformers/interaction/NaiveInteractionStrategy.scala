// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.interaction

import fr.polytechnique.cmap.cnam.etl.events.{Event, Interaction}

object NaiveInteractionStrategy extends InteractionTransformer with Serializable {

  override def transform(exposures: Exposures): Interactions = {
    exposures.cache()
    val elevatedExposures = exposures.transform(elevateExposure).cache()

    val interactionLevel1 = exposures
      .transform(
        elevateToLevel1Interaction(
          elevatedExposures.transform(findInteractionPeriods).transform(findLongestUninterruptedInteractionPeriods)
        )
      )
    val interactionLevel2 = elevatedExposures.transform(fromElevatedExposuresToSecondLevelInteractions)
    interactionLevel2.union(interactionLevel1)
  }

  def elevateExposure(exposures: Exposures): ElevatedExposures = {
    val sqlCtx = exposures.sqlContext
    import sqlCtx.implicits._

    val right = exposures
    // WARN: The next line is only to break the lineage of the exposure dataset and allow a cross join to happen
    val left = exposures.sparkSession.sqlContext.createDataset[Exposure_](exposures.rdd)
    left.joinWith(
      right,
      left(Event.Columns.PatientID) === right(Event.Columns.PatientID) &&
        right(Event.Columns.Start) < left(Event.Columns.End) &&
        right(Event.Columns.Start) >= left(Event.Columns.Start) &&
        !left(Event.Columns.Value).contains(right(Event.Columns.Value)),
      "inner"
    )

  }

  def fromElevatedExposuresToSecondLevelInteractions(elevatedExposures: ElevatedExposures): Interactions = {
    val sqlCtx = elevatedExposures.sqlContext
    import sqlCtx.implicits._

    elevatedExposures.map(fromElevatedExposureToInteraction).distinct()
  }

  def findInteractionPeriods(tupleExposures: ElevatedExposures): InterActionPeriods = {
    val sqlCtx = tupleExposures.sqlContext
    import sqlCtx.implicits._
    tupleExposures.flatMap(defineInteractionPeriod)
  }

  def findLongestUninterruptedInteractionPeriods: InterActionPeriods => InterActionPeriods = {
    input =>
      val sqlCtx = input.sqlContext
      import sqlCtx.implicits._

      input
        .groupByKey(i => (i.patientID, i.value))
        .flatMapGroups((_, s) => findLongestUninterruptedInteractionPeriod(s.toList.sortBy(_.start), List.empty))
  }

  def elevateToLevel1Interaction(interActionPeriod: InterActionPeriods)(exposures: Exposures): Interactions = {

    val sqlCtx = exposures.sqlContext
    import sqlCtx.implicits._

    exposures
      .joinWith(
        interActionPeriod,
        interActionPeriod(Event.Columns.Start).between(exposures(Event.Columns.Start), exposures(Event.Columns.End)) &&
          exposures(Event.Columns.Value) === interActionPeriod(Event.Columns.Value) &&
          exposures(Event.Columns.PatientID) === interActionPeriod(Event.Columns.PatientID),
        "left"
      )
      .groupByKey(e => e._1)
      .flatMapGroups(
        (exposure, s) =>
          delimitExposurePeriodByItsInteractionPeriods(
            RightRemaining(exposure), s.map(_._2).toList.sortBy(_.start), List.empty
          )
      )
      .map(e => Interaction(e))
  }

}

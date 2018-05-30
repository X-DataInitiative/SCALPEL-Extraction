package fr.polytechnique.cmap.cnam.study.rosiglitazone.outcomes

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.transformers.outcomes.OutcomesTransformer
import fr.polytechnique.cmap.cnam.study.rosiglitazone.RosiglitazoneStudyCodes
import fr.polytechnique.cmap.cnam.util.collections

object HeartFailure extends OutcomesTransformer with RosiglitazoneStudyCodes {

  override val outcomeName: String = OutcomeDefinition.HeartFailure.outcomeName

  private val DP = MainDiagnosis.category
  private val DR = LinkedDiagnosis.category
  private val DAS = AssociatedDiagnosis.category

  private val diagHeartFailure = diagCodeHeartFailure
  private val diagHeartComplication = diagCodeHeartComplication

  def checkHeartFailure(events: Seq[Event[Diagnosis]]): Boolean = {
    import collections.implicits._

    events.exists (ev => ev.checkValue(DP, diagHeartFailure)) ||
      events.existAll(
        ev => ev.checkValue(DP, diagHeartComplication),
        ev => ev.checkValue(DR, diagHeartFailure) || ev.checkValue(DAS, diagHeartFailure)
      )
  }

  def findOutcomesPerHospitalization(events: Seq[Event[Diagnosis]]): Seq[Event[Outcome]] = {
    events
      .groupBy(_.groupID)
      .collect {
        case (_, groupedEvents) if checkHeartFailure(groupedEvents) =>
          Outcome(groupedEvents.head.patientID, outcomeName, groupedEvents.head.start)
      }
      .toStream
  }

  def transform(extracted : Dataset[Event[Diagnosis]]): Dataset[Event[Outcome]] = {

    import extracted.sqlContext.implicits._

    extracted
      .groupByKey(ev => (ev.patientID, ev.start))
      .flatMapGroups{
        case (_, groupedEvents) => findOutcomesPerHospitalization(groupedEvents.toStream)
      }
  }
}

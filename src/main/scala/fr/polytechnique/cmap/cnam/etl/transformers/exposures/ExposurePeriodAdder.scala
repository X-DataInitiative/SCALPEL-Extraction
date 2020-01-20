// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import me.danielpes.spark.datetime.implicits._
import me.danielpes.spark.datetime.{Period => Duration}
import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.datatypes._
import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event, Exposure, FollowUp}
import fr.polytechnique.cmap.cnam.util.functions._


sealed abstract class ExposurePeriodAdder(val startDelay: Duration) extends Serializable {

  def toExposure(followUps: Dataset[Event[FollowUp]])(drugs: Dataset[Event[Drug]]): Dataset[Event[Exposure]]

  def delayStart(followUps: Dataset[Event[FollowUp]])(drugs: Dataset[Event[Drug]]): Dataset[Event[Drug]] = {
    val sqlCtx = drugs.sqlContext
    import sqlCtx.implicits._

    drugs
      .joinWith(followUps, drugs(Event.Columns.PatientID) === followUps(Event.Columns.PatientID), "inner")
      .flatMap(e => delayStartOfDrugPurchase(e._1, e._2, startDelay))
  }

  def delayStartOfDrugPurchase(
    drug: Event[Drug],
    followUp: Event[FollowUp],
    startDelay: Duration): Option[Event[Drug]] = {
    val delayedStart = if (followUp.end.get.after(startDelay.+(drug.start).get)) drug.start + startDelay else None
    delayedStart.map(Drug(drug.patientID, drug.groupID, drug.value, drug.weight, _, drug.end))
  }
}


final case class LimitedExposureAdder(
  override val startDelay: Duration,
  endDelay: Duration,
  endThresholdGc: Duration,
  endThresholdNgc: Duration,
  toExposureStrategy: ExposureDurationStrategy) extends ExposurePeriodAdder(startDelay) {

  override def toExposure(followUps: Dataset[Event[FollowUp]])
    (drugs: Dataset[Event[Drug]]): Dataset[Event[Exposure]] = {
    drugs.transform(delayStart(followUps)).transform(toExposure)
  }

  def toExposure(drugs: Dataset[Event[Drug]]): Dataset[Event[Exposure]] = {
    val sqlCtx = drugs.sqlContext
    import sqlCtx.implicits._
    drugs
      .map(fromDrugToExposureDuration)
      .groupByKey(ep => (ep.patientID, ep.value))
      .flatMapGroups((_, eds) => combineExposureDurations(eds))
      .map(ed => ed.copy(period = ed.period.copy(end = ed.period.end - endDelay get)))
      .map(e => toExposureStrategy(e))
  }

  def combineExposureDurations(exposureDurations: Iterator[ExposureDuration]): List[ExposureDuration] = {
    val sortedExposureDurations = exposureDurations.toList.sortBy(_.period.start).map(LeftRemainingPeriod(_))
    Addable.combineAddables(sortedExposureDurations.head.toRight, sortedExposureDurations.drop(1), List.empty)
      .map(_.e)
  }

  def fromDrugToExposureDuration(drug: Event[Drug]): ExposureDuration = {
    val duration = fromConditioningToDuration(drug.weight)
    ExposureDuration(
      drug.patientID,
      drug.value,
      Period(drug.start, (drug.start + Duration(milliseconds = duration).+(endDelay)).get),
      duration
    )
  }

  def fromConditioningToDuration(weight: Double): Long = weight match {
    case 1 => endThresholdGc.totalMilliseconds
    case _ => endThresholdNgc.totalMilliseconds
  }
}

final case class UnlimitedExposureAdder(
  override val startDelay: Duration,
  minPurchases: Int,
  purchasesWindow: Duration
) extends ExposurePeriodAdder(startDelay) {

  override def toExposure(followUps: Dataset[Event[FollowUp]])
    (drugs: Dataset[Event[Drug]]): Dataset[Event[Exposure]] = {
    val sqlCtx = drugs.sqlContext
    import sqlCtx.implicits._
    val intermediate = drugs
      .groupByKey(ep => (ep.patientID, ep.value))
      .flatMapGroups((_, eds) => fromDrugsToExposureCandidate(eds))

    intermediate
      .joinWith(followUps, intermediate(Event.Columns.PatientID) === followUps(Event.Columns.PatientID), "inner")
      .filter(e => e._1.start.before(e._2.end.get))
      .map(e => Exposure(e._1.patientID, e._1.groupID, e._1.value, 1D, e._1.start, e._2.end))
  }

  def fromDrugsToExposureCandidate(drugs: Iterator[Event[Drug]]): TraversableOnce[Event[Drug]] =
    drugs.toList.sortBy(_.start).toStream.sliding(minPurchases, 1).find(ds => ds.size >= minPurchases & inWindow(ds))
      .map(_.reverse.head)


  def inWindow(drugs: Stream[Event[Drug]]): Boolean = {
    val first = drugs.headOption
    val last = drugs.reverse.headOption
    first match {
      case None => false
      case Some(e) =>
        val reachTs = (e.start + purchasesWindow).get
        reachTs.after(last.get.start) | reachTs.equals(last.get.start)

    }
  }

}

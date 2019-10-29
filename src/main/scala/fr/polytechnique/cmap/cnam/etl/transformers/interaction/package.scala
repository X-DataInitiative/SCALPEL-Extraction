// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers

import java.sql.Timestamp
import scala.annotation.tailrec
import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Event, Exposure, Interaction}

package object interaction {
  case class InterActionPeriod(patientID: String, value: String, start: Timestamp, end: Timestamp)

  implicit val ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
    def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
  }

  type Interaction_ = Event[Interaction]
  type Exposure_ = Event[Exposure]
  type ElevatedExposure = (Exposure_, Exposure_)

  type Interactions = Dataset[Interaction_]
  type Exposures = Dataset[Exposure_]
  type ElevatedExposures = Dataset[ElevatedExposure]
  type InterActionPeriods = Dataset[InterActionPeriod]

  val min = (x: Timestamp, y: Timestamp) => if (ordered.lteq(x, y)) x else y
  val max = (x: Timestamp, y: Timestamp) => if (ordered.gteq(x, y)) x else y

  val formatValue = (tuple: ElevatedExposure) => List(tuple._1.value, tuple._2.value).sorted
    .reduce((l, r) => l.concat("_").concat(r))

  val fromElevatedExposureToInteraction: ElevatedExposure => Interaction_ = e => e._1.end.get.compareTo(e._2.end.get) match {
    case 0 => Interaction(e._1.patientID, e._1.groupID, formatValue(e), 2.0, e._2.start, e._1.end)
    case 1 => Interaction(e._1.patientID, e._1.groupID, formatValue(e), 2.0, e._2.start, e._2.end)
    case -1 => Interaction(e._1.patientID, e._1.groupID, formatValue(e), 2.0, e._2.start, e._1.end)
  }

  val defineInteractionPeriod = (e: ElevatedExposure) =>
    if (e._1.end.get.compareTo(e._2.end.get) <= 0) {
      List(
        InterActionPeriod(e._1.patientID, e._1.value, e._2.start, e._1.end.get),
        InterActionPeriod(e._1.patientID, e._2.value, e._2.start, e._1.end.get)
      )
    } else {
      List(
        InterActionPeriod(e._1.patientID, e._1.value, e._2.start, e._2.end.get),
        InterActionPeriod(e._1.patientID, e._2.value, e._2.start, e._2.end.get)
      )
    }


  val areInteractionPeriodsOverlaping = (x: InterActionPeriod, y: InterActionPeriod)
  => (x.patientID == y.patientID) && (x.value == y.value) && (x.end.compareTo(y.start) >= 0)

  val mergeOverlapingInteractionPeriods = (l: InterActionPeriod, r: InterActionPeriod)
  => InterActionPeriod(l.patientID, l.value, min(l.start, r.start), max(l.end, r.end))

  @tailrec
  def findLongestUninterruptedInteractionPeriod(
    i: List[InterActionPeriod],
    acc: List[InterActionPeriod]): List[InterActionPeriod] = {
    i match {
      case Nil => acc
      case e :: Nil => e :: acc
      case s :: e :: rest => if (areInteractionPeriodsOverlaping(s, e)) {
        findLongestUninterruptedInteractionPeriod(mergeOverlapingInteractionPeriods(s, e) :: rest, acc)
      } else {
        findLongestUninterruptedInteractionPeriod(e :: rest, s :: acc)
      }
    }
  }

  val reduceInteractionPeriodFromExposurePeriod: (RightRemaining, InterActionPeriod) => RemainingExposure = {
    (rr, interaction) =>
      val exposure = rr.e
      (exposure.start.compareTo(interaction.start), exposure.end.get.compareTo(interaction.end)) match {
        case (0, 0) => NullRemaining
        case (0, 1) =>
          RightRemaining(Exposure(exposure.patientID, exposure.groupID, exposure.value, exposure.weight, interaction.end, exposure.end))
        case (-1, 0) =>
          LeftRemaining(Exposure(exposure.patientID, exposure.groupID, exposure.value, exposure.weight, exposure.start, Some(interaction.start)))
        case (-1, 1) => ExplodedRemaining(
          LeftRemaining(Exposure(exposure.patientID, exposure.groupID, exposure.value, exposure.weight, exposure.start, Some(interaction.start))),
          RightRemaining(Exposure(exposure.patientID, exposure.groupID, exposure.value, exposure.weight, interaction.end, exposure.end))
        )
      }
  }

  @tailrec
  def delimitExposurePeriodByItsInteractionPeriods(
    rr: RightRemaining,
    interactionsPeriods: List[InterActionPeriod],
    acc: List[Exposure_]): List[Exposure_] = {
    interactionsPeriods match {
      case _@null :: Nil => rr.e :: acc // This happens because of the Left Join
      case Nil => rr.e :: acc
      case i :: rest => reduceInteractionPeriodFromExposurePeriod(rr, i) match {
        case NullRemaining => acc
        case LeftRemaining(e) => e :: acc
        case r: RightRemaining => delimitExposurePeriodByItsInteractionPeriods(r, rest, acc)
        case ExplodedRemaining(LeftRemaining(e), r) => delimitExposurePeriodByItsInteractionPeriods(r, rest, e :: acc)
      }
    }
  }

  sealed trait RemainingExposure

  case class RightRemaining(e: Exposure_) extends RemainingExposure
  case class LeftRemaining(e: Exposure_) extends RemainingExposure
  case class ExplodedRemaining(l: LeftRemaining, r: RightRemaining) extends RemainingExposure
  case object NullRemaining extends RemainingExposure
}

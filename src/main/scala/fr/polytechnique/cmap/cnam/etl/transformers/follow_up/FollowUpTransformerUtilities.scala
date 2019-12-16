package fr.polytechnique.cmap.cnam.etl.transformers.follow_up

import java.sql.Timestamp
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up.Columns.EndReasons
import fr.polytechnique.cmap.cnam.util.datetime.implicits.addMonthsToRichTimestamp


object FollowUpTransformerUtilities {


  case class PatientDates(
    patientID: String,
    deathDate: Option[Timestamp],
    followUpStart: Option[Timestamp],
    observationEnd: Option[Timestamp])


  case class TrackLossDate(
    patientID: String,
    trackloss: Option[Timestamp])

  case class FollowUpEnd(reason: String, date: Option[Timestamp])

  abstract sealed class FollowUpEndReason {
    val reason: String
    val date: Option[Timestamp]

    def compare(that: FollowUpEndReason): Int = {

      (this.date.get compareTo that.date.get) match {
        case 0 => (this.reason, that.reason) match {
          case ("Death", _) => 1
          case (_, "Death") => -1
          case ("Disease", "ObservationEnd") => 1
          case ("ObservationEnd", "Disease") => -1
          case (_, _) => 1
        }
        case c => c
      }
    }
  }

  object FollowUpEndReason {

    implicit def ord[A <: FollowUpEndReason]: Ordering[A] = Ordering.by((_: A).date.get)

    implicit def ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
      def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
    }


  }

  case class DeathReason(
    reason: String = EndReasons.Death.toString,
    date: Option[Timestamp]) extends FollowUpEndReason with Ordered[FollowUpEndReason]

  case class DiseaseReason(
    reason: String = EndReasons.Disease.toString,
    date: Option[Timestamp]) extends FollowUpEndReason with Ordered[FollowUpEndReason]

  case class TrackLossReason(
    reason: String = EndReasons.Trackloss.toString,
    date: Option[Timestamp]) extends FollowUpEndReason with Ordered[FollowUpEndReason]

  case class ObservationEndReason(
    reason: String = EndReasons.ObservationEnd.toString,
    date: Option[Timestamp]) extends FollowUpEndReason with Ordered[FollowUpEndReason]


  val correctedStart: (Timestamp, Option[Timestamp], Int) => Option[Timestamp] =
    (start: Timestamp, end: Option[Timestamp], delayMonths: Int) => {
      val st: Timestamp = addMonthsToRichTimestamp(delayMonths, start)
      if (st.before(end.get)) Some(st) else None

    }

  val tracklossDateCorrected: (Timestamp, Timestamp) => Option[Timestamp] =
    (start: Timestamp, followUpStart: Timestamp) => {
      if (start.after(followUpStart)) Some(start) else None
    }

  def endReason(
    death: DeathReason,
    disease: DiseaseReason,
    trackloss: TrackLossReason,
    observation: ObservationEndReason): FollowUpEnd = {
    val followUpEndReason = Seq(death, disease, trackloss, observation).filter(e => e.date.nonEmpty).min
    FollowUpEnd(followUpEndReason.reason, followUpEndReason.date)
  }


}

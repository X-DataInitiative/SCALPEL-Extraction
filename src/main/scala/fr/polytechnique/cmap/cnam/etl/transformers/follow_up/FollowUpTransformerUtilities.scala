// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.follow_up

import java.sql.Timestamp
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up.Columns.EndReasons
import fr.polytechnique.cmap.cnam.util.datetime.implicits.addMonthsToRichTimestamp

/** Factory for FollowUp utilities. */
object FollowUpTransformerUtilities {

  /** It saves patients with need dates
   *
   * @param patientID      The value patientID from dataset
   * @param deathDate      The value deathDate from dataset
   * @param followUpStart  The value followUpStart from dataset
   * @param observationEnd The value observationEnd from dataset
   */
  case class PatientDates(
    patientID: String,
    deathDate: Option[Timestamp],
    followUpStart: Option[Timestamp],
    observationEnd: Option[Timestamp])

  /** It saves patients with their trackloss date
   *
   * @param patientID The value patientID from dataset
   * @param trackloss The value trackloss from dataset
   */
  case class TrackLossDate(
    patientID: String,
    trackloss: Option[Timestamp])

  /** It saves the follow up's end's reason and its date
   *
   * @param reason The reason of the end
   * @param date   The date of end
   */
  case class FollowUpEnd(reason: String, date: Option[Timestamp])

  /** Class to stores the  methods to compare end reasons and dates between
   * and return the correct value for end reason
   */
  abstract sealed class FollowUpEndReason {
    val reason: String
    val date: Option[Timestamp]

    /** It takes a FollowUpEndReason type class and compare the dates,
     * if they are the same, then compare the end reasons to return
     * the correct according to priority.
     * Death reason are the most priority.
     *
     * @param that The class of FollowUpEndReason type
     * @return the correct FollowUpEndReason class
     */
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

  /** Implicit ordering for the timestamps in FollowUpEndReason type case class
   *
   * The filter to avoid empty dates its mandatory
   * Exemple: Seq(death, disease, trackloss, observation).filter(e => e.date.nonEmpty).min
   *
   */
  object FollowUpEndReason {

    implicit def ord[A <: FollowUpEndReason]: Ordering[A] = Ordering.by((_: A).date.get)

    implicit def ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
      def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
    }

  }

  /** It stores death reason and its date
   *
   * @param reason The value "Death"
   * @param date   The value deathDate from dataset
   */
  case class DeathReason(
    reason: String = EndReasons.Death.toString,
    date: Option[Timestamp]) extends FollowUpEndReason with Ordered[FollowUpEndReason]

  /** It stores disease reason and its date
   *
   * @param reason The value "Disease"
   * @param date   The value start from dataset Outcome
   */
  case class DiseaseReason(
    reason: String = EndReasons.Disease.toString,
    date: Option[Timestamp]) extends FollowUpEndReason with Ordered[FollowUpEndReason]

  /** It stores trackloss reason and its date
   *
   * @param reason The value "Trackloss"
   * @param date   The value trackloss from dataset TrackLossDate
   */
  case class TrackLossReason(
    reason: String = EndReasons.Trackloss.toString,
    date: Option[Timestamp]) extends FollowUpEndReason with Ordered[FollowUpEndReason]

  /** It stores observation end reason and its date
   *
   * @param reason The value "ObservationEnd"
   * @param date   The value observationEnd from dataset PatientDates
   */
  case class ObservationEndReason(
    reason: String = EndReasons.ObservationEnd.toString,
    date: Option[Timestamp]) extends FollowUpEndReason with Ordered[FollowUpEndReason]

  /** It returns the date later add delayMonths value from
   * config passes through FollowUpTransformer class
   */
  val correctedStart: (Timestamp, Option[Timestamp], Int) => Option[Timestamp] =
    (start: Timestamp, end: Option[Timestamp], delayMonths: Int) => {
      val st: Timestamp = addMonthsToRichTimestamp(delayMonths, start)
      if (st.before(end.get)) Some(st) else None

    }

  /** It returns start date when after follow Up Start otherwise None */
  val tracklossDateCorrected: (Timestamp, Timestamp) => Option[Timestamp] =
    (start: Timestamp, followUpStart: Timestamp) => {
      if (start.after(followUpStart)) Some(start) else None
    }

  /** It takes all FollowUpEndReason type class and return the min of them
   *
   * @param death       A DeathReason class
   * @param disease     A DiseaseReason class
   * @param trackloss   A TrackLossReason class
   * @param observation A ObservationEndReason class
   * @return FollowUpEnd class
   */
  def endReason(
    death: DeathReason,
    disease: DiseaseReason,
    trackloss: TrackLossReason,
    observation: ObservationEndReason): FollowUpEnd = {
    val followUpEndReason = Seq(death, disease, trackloss, observation).filter(e => e.date.nonEmpty).min
    FollowUpEnd(followUpEndReason.reason, followUpEndReason.date)
  }

}

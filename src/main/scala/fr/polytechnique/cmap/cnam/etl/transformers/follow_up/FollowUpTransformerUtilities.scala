// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.follow_up

import java.sql.Timestamp
import fr.polytechnique.cmap.cnam.util.datetime.implicits.addMonthsToRichTimestamp

/** Factory for FollowUp utilities. */
object FollowUpTransformerUtilities {

  /** It saves patients with need dates.
   *
   * @param patientID      The value patientID from dataset.
   * @param deathDate      The value deathDate from dataset.
   * @param followUpStart  The value followUpStart from dataset.
   * @param observationEnd The value observationEnd from dataset.
   */
  private[follow_up] case class PatientDates(
    patientID: String,
    deathDate: Option[Timestamp],
    followUpStart: Option[Timestamp],
    observationEnd: Option[Timestamp])

  /** It saves patients with their trackloss date.
   *
   * @param patientID The value patientID from dataset.
   * @param trackloss The value trackloss from dataset.
   */
  private[follow_up] case class TrackLossDate(
    patientID: String,
    trackloss: Option[Timestamp])

  /** It saves the follow up's end's reason and its date.
   *
   * @param reason A string value of the reason of the end.
   * @param date   The date of the end of follow up.
   */
  private[follow_up] case class FollowUpEnd(reason: String, date: Option[Timestamp])

  /** It stores the list of reasons. */
  private[follow_up] sealed trait EndReason extends Enumeration {
    val Death, Trackloss, ObservationEnd = Value
    val endReason: String
  }

  /** It's an object to store Death as endReason. */
  private[follow_up] case object Death extends EndReason {
    val endReason = Death
      .toString
  }

  /** It's an object to store Trackloss as endReason. */
  private[follow_up] case object Trackloss extends EndReason {
    val endReason = Trackloss
      .toString
  }

  /** It's an object to store ObservationEnd as endReason. */
  private[follow_up] case object ObservationEnd extends EndReason {
    val endReason = ObservationEnd
      .toString
  }

  /** It's need to  store the compare  method
   * which allow use the min function with
   * the FollowUpEndReason class type.
   *
   * @param endReason An object of type EndReason.
   */
  private[follow_up] abstract sealed class FollowUpEndReason(val endReason: EndReason) {
    val date: Option[Timestamp]

    /** It takes a FollowUpEndReason type class and compare the dates,
     * if they are the same, then compare the end reasons to return
     * the correct according to priority.
     * If two reasons are the same date,
     * Death is the first option, if Death is not present, the second one is Trackloss
     * and the third option is ObservationEnd.
     *
     * @param that The class of FollowUpEndReason type.
     * @return The correct FollowUpEndReason class.
     */
    def compare(that: FollowUpEndReason): Int = {

      (this.date.get compareTo that.date.get) match {
        case 0 => (this.endReason.endReason, that.endReason.endReason) match {
          case ("Death", _) => 1
          case (_, "Death") => -1
          case ("Trackloss", _) => 1
          case (_, "Trackloss") => -1
        }
        case c => c
      }
    }
  }

  /** It stores the implicit Ordering needed to use the min function in FollowUpEndReason types class. */
  object FollowUpEndReason {

    import fr.polytechnique.cmap.cnam.util.datetime.implicits.ordered

    /** Implicit ordering for the timestamps in FollowUpEndReason type case class.
     *
     * The filter to avoid empty dates its mandatory.
     * Example: Seq(death, disease, trackloss, observation).filter(e => e.date.nonEmpty).min
     *
     */
    implicit def ord[A <: FollowUpEndReason]: Ordering[A] = Ordering.by((_: A).date.get)

  }

  /** It stores death reason and its date.
   *
   * @param date The value deathDate from dataset.
   */
  case class DeathReason(
    date: Option[Timestamp]) extends FollowUpEndReason(Death) with Ordered[FollowUpEndReason]

  /** It stores trackloss reason and its date.
   *
   * @param date The value trackloss from dataset TrackLossDate.
   */
  case class TrackLossReason(
    date: Option[Timestamp]) extends FollowUpEndReason(Trackloss) with Ordered[FollowUpEndReason]

  /** It stores observation end reason and its date.
   *
   * @param date The value observationEnd from dataset PatientDates.
   */
  case class ObservationEndReason(
    date: Option[Timestamp]) extends FollowUpEndReason(ObservationEnd) with Ordered[FollowUpEndReason]

  /** It returns the date later add delayMonths value from
   * config passes through FollowUpTransformer class.
   */
  val correctedStart: (Timestamp, Option[Timestamp], Int) => Option[Timestamp] =
    (start: Timestamp, end: Option[Timestamp], delayMonths: Int) => {
      val st: Timestamp = addMonthsToRichTimestamp(delayMonths, start)
      if (st.before(end.get)) Some(st) else None
    }

  /** It returns start date when after follow Up Start otherwise None. */
  val tracklossDateCorrected: (Timestamp, Timestamp) => Option[Timestamp] =
    (start: Timestamp, followUpStart: Timestamp) => {
      if (start.after(followUpStart)) Some(start) else None
    }

  /** It takes all FollowUpEndReason type class and return the min of them.
   *
   * @param death       A DeathReason class.
   * @param trackloss   A TrackLossReason class.
   * @param observation A ObservationEndReason class.
   * @return FollowUpEnd class.
   */
  def endReason(
    death: DeathReason,
    trackloss: TrackLossReason,
    observation: ObservationEndReason): FollowUpEnd = {
    val followUpEndReason = Seq(death, trackloss, observation).filter(e => e.date.nonEmpty).min
    FollowUpEnd(followUpEndReason.endReason.endReason, followUpEndReason.date)
  }

}

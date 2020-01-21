// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.follow_up

import java.sql.Timestamp
import scala.util.Try
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.patients.Patient

/** It allows create a followUp dataset using the dataset of
 * Dataset[(Patient, Event[ObservationPeriod])],
 * Dataset[Event[Molecule]] ( This dataset is not used in the treatment and it should be removed in further versions),
 * Dataset[Event[Outcome]]( This dataset is not used in the treatment and it should be removed in further versions) and
 * Dataset[Event[Trackloss]].
 *
 * @param config A config object tha contains the need values to set the parameters of study.
 */
class FollowUpTransformer(config: FollowUpTransformerConfig) {

  /** The main method in this transformation class, It combines multiple basic Events to form a FollowUp Dataset.
   *
   * @param patients      A dataset that contains a dataset of [[fr.polytechnique.cmap.cnam.etl.patients.Patient]] joined with
   *                      a dataset of [[fr.polytechnique.cmap.cnam.etl.events.ObservationPeriod]].
   * @param dispensations A dataset of [[fr.polytechnique.cmap.cnam.etl.events.Molecule]].
   * @param outcomes      A dataset of [[fr.polytechnique.cmap.cnam.etl.events.Outcome]].
   * @param tracklosses   A dataset of [[fr.polytechnique.cmap.cnam.etl.events.Trackloss]].
   * @return A dataset of Event[FollowUp] type ([[fr.polytechnique.cmap.cnam.etl.events.FollowUp]]).
   */
  def transform(
    patients: Dataset[(Patient, Event[ObservationPeriod])],
    dispensations: Dataset[Event[Molecule]],
    outcomes: Dataset[Event[Outcome]],
    tracklosses: Dataset[Event[Trackloss]]): Dataset[Event[FollowUp]] = {

    import patients.sparkSession.implicits._
    import FollowUpTransformerUtilities._
    import Columns._


    val delayMonths = config.delayMonths

    /** It takes the Dataset[(Patient, Event[ObservationPeriod])] and perform several transformations:
     * 1. Extract th patientId value.
     * 2. Correct the observationPeriod start date plus delayMonth value comparing to observationPeriod end date.
     * 3. Calculate the min of dates.
     * 4 Return a PatientDates dataset [[fr.polytechnique.cmap.cnam.etl.transformers.follow_up.FollowUpTransformerUtilities.PatientDates]].
     */
    val patientDates: Dataset[PatientDates] = patients
      .map { e =>
        PatientDates(
          e._1.patientID,
          e._1.deathDate,
          correctedStart(e._2.start, e._2.end, delayMonths),
          e._2.end
        )
      }
      .filter(e => e.followUpStart.nonEmpty)
      .groupBy(col(PatientID))
      .agg(
        min(DeathDate).as(DeathDate),
        min(FollowUpStart).as(FollowUpStart),
        min(Columns.ObservationEnd).as(Columns.ObservationEnd)
      )
      .map(
        e => PatientDates(
          e.getAs[String](PatientID),
          Option(e.getAs[Timestamp](DeathDate)),
          Option(e.getAs[Timestamp](FollowUpStart)),
          Option(e.getAs[Timestamp](Columns.ObservationEnd))
        )
      )

    /** It takes patientDates dataset and join with tracklosses dataset performing the algorithm as follow:
     * 1. Extract the patientId value and correct the trackloss date comparing with followUpStart date.
     * 2. Filter corrected empty dates.
     * 3. Extract the min of tracklossDate.
     * 4. Return a TrackLossDate [[fr.polytechnique.cmap.cnam.etl.transformers.follow_up.FollowUpTransformerUtilities.TrackLossDate]].
     */
    val tracklossDates: Dataset[TrackLossDate] = patientDates
      .joinWith(tracklosses, tracklosses.col(PatientID) === patientDates.col(PatientID))
      .map(e => TrackLossDate(e._2.patientID, tracklossDateCorrected(e._2.start, e._1.followUpStart.get)))
      .filter(e => e.trackloss.nonEmpty)
      .groupBy(col(PatientID))
      .agg(
        min(TracklossDate).as(TracklossDate)
      )
      .map(e => TrackLossDate(e.getAs[String](PatientID), Option(e.getAs[Timestamp](TracklossDate))))

    /** It joins patientDates dataset with tracklossDates dataset carrying out the following algorithm :
     * 1. Retrieve the trackloss date if exist, None otherwise.
     * 2. Using the death date, the trackloss date and the observation end date it calculates through
     * [[fr.polytechnique.cmap.cnam.etl.transformers.follow_up.FollowUpTransformerUtilities.endReason]] the follow up's
     * end date and reason.
     * 3. It filters the empty followUp end dates.
     * 4. Return a FollowUp [[fr.polytechnique.cmap.cnam.etl.events.FollowUp]].
     */
    patientDates
      .joinWith(tracklossDates, tracklossDates.col(PatientID) === patientDates.col(PatientID), "left_outer")
      .map { e =>
        val trackloss: Option[Timestamp] = Try(e._2.trackloss).getOrElse(None)

        val followUpEndReason = endReason(
          DeathReason(date = e._1.deathDate),
          TrackLossReason(date = trackloss),
          ObservationEndReason(date = e._1.observationEnd)
        )
        FollowUp(e._1.patientID, followUpEndReason.reason, e._1.followUpStart.get, followUpEndReason.date.get)
      }.filter(e => e.end.nonEmpty)
  }
}

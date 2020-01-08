// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.follow_up

import java.sql.Timestamp
import scala.util.Try
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.patients.Patient


class FollowUpTransformer(config: FollowUpTransformerConfig) {


  def transform(
    patients: Dataset[(Patient, Event[ObservationPeriod])],
    dispensations: Dataset[Event[Molecule]],
    outcomes: Dataset[Event[Outcome]],
    tracklosses: Dataset[Event[Trackloss]]): Dataset[Event[FollowUp]] = {

    import patients.sparkSession.implicits._
    import FollowUpTransformerUtilities._
    import Columns._


    val delayMonths = config.delayMonths

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

    val tracklossDates: Dataset[TrackLossDate] = patientDates
      .joinWith(tracklosses, tracklosses.col(PatientID) === patientDates.col(PatientID))
      .map(e => TrackLossDate(e._2.patientID, tracklossDateCorrected(e._2.start, e._1.followUpStart.get)))
      .filter(e => e.trackloss.nonEmpty)
      .groupBy(col(PatientID))
      .agg(
        min(TracklossDate).as(TracklossDate)
      )
      .map(e => TrackLossDate(e.getAs[String](PatientID), Option(e.getAs[Timestamp](TracklossDate))))

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

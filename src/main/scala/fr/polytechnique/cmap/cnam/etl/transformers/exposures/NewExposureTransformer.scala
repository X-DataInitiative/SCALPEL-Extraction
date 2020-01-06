// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event, Exposure, FollowUp}
import fr.polytechnique.cmap.cnam.util.functions._

class NewExposureTransformer(config: NewExposuresTransformerConfig) extends Serializable {

  def transform(followUps: Dataset[Event[FollowUp]])(drugs: Dataset[Event[Drug]]): Dataset[Event[Exposure]] = {
    drugs
      .transform(config.exposurePeriodAdder.toExposure(followUps))
      .transform(regulateWithFollowUps(followUps))
  }

  def regulateWithFollowUps(followUps: Dataset[Event[FollowUp]])
    (exposures: Dataset[Event[Exposure]]): Dataset[Event[Exposure]] = {
    val sqlCtx = followUps.sparkSession.sqlContext
    import sqlCtx.sparkSession.implicits._
    exposures
      .joinWith(followUps, exposures(Event.Columns.PatientID) === followUps(Event.Columns.PatientID), "inner")
      .flatMap(
        e => {
          val followUp = e._2
          val exposure = e._1
          regulateWithFollowUp(exposure)(followUp)
        }
      )
  }

  /** Returns None if Exposure happens before or after the FollowUp, otherwise return Some(Exposure) where the Exposure
    * start as earliest as the FollowUp start and ends as latest as FollowUp end.
    *
    * @param exposure to regulate.
    * @param followUp to regulate with.
    * @return Option on an Exposure.
    */
  def regulateWithFollowUp(exposure: Event[Exposure])(followUp: Event[FollowUp]): TraversableOnce[Event[Exposure]] =
    controlPrevalentExposure(exposure)(followUp)
      .flatMap(controlDelayedExposure(_)(followUp))
      .map(regulateExposureStartWithFollowUp(_)(followUp))
      .map(regulateExposureEndWithFollowUp(_)(followUp))

  /** Returns None if Exposure happens entirely before the FollowUp, otherwise returns Some(Exposure) with Exposure as
    * is.
    *
    * @param exposure the exposure to check.
    * @param followUp the followup event to control with.
    * @return an Option of Event Exposure
    */
  protected[exposures] def controlPrevalentExposure(exposure: Event[Exposure])
    (followUp: Event[FollowUp]): TraversableOnce[Event[Exposure]] = {
    if (exposure.end.get.before(followUp.start)) {
      None
    } else {
      Some(exposure)
    }
  }

  /** Returns None if Exposure happens entirely before the FollowUp, otherwise returns Some(Exposure) with Exposure as
    * is.
    *
    * @param exposure the exposure to check.
    * @param followUp the followup event to control with.
    * @return an Option of Event Exposure
    */
  protected[exposures] def controlDelayedExposure(exposure: Event[Exposure])
    (followUp: Event[FollowUp]): TraversableOnce[Event[Exposure]] = {
    if (exposure.start.after(followUp.end.get)) {
      None
    } else {
      Some(exposure)
    }
  }


  /** Make Exposure end as latest as FollowUp end.
    *
    * @param exposure exposure to regulate
    * @param followUp the followup event to regulate with.
    * @return Event[Exposure]
    */
  protected[exposures] def regulateExposureEndWithFollowUp(exposure: Event[Exposure])
    (followUp: Event[FollowUp]): Event[Exposure] =
    Exposure(
      exposure.patientID,
      exposure.groupID,
      exposure.value,
      exposure.weight,
      exposure.start,
      exposure.end.map(ex => followUp.end.map(fu => min(ex, fu))).get
    )


  /** Make Exposure start as earliest as FollowUp start.
    *
    * @param exposure exposure to regulate
    * @param followUp the followup event to regulate with.
    * @return Event[Exposure]
    */
  protected[exposures] def regulateExposureStartWithFollowUp(exposure: Event[Exposure])
    (followUp: Event[FollowUp]): Event[Exposure] =
    Exposure(
      exposure.patientID,
      exposure.groupID,
      exposure.value,
      exposure.weight,
      max(exposure.start, followUp.start),
      exposure.end
    )
}

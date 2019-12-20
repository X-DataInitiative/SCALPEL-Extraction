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
    import sqlCtx.implicits._
    exposures
      .joinWith(followUps, exposures(Event.Columns.PatientID) === followUps(Event.Columns.PatientID), "inner")
      .flatMap(
        e => {
          val followUp = e._2
          val exposure = e._1
          regulateExposureWithFollowUp(exposure, followUp)
        }
      )
  }

  def regulateExposureWithFollowUp(
    exposure: Event[Exposure],
    followUp: Event[FollowUp]): TraversableOnce[Event[Exposure]] = {
    if (exposure.start.after(followUp.end.get)) {
      None
    } else {
      Some(
        Exposure(
          exposure.patientID,
          exposure.groupID,
          exposure.value,
          exposure.weight,
          max(exposure.start, followUp.start),
          exposure.end.map(ex => followUp.end.map(fu => min(ex, fu))).get
        )
      )
    }
  }
}

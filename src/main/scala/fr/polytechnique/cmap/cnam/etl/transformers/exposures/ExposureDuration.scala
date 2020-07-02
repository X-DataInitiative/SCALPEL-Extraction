// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import me.danielpes.spark.datetime.implicits._
import me.danielpes.spark.datetime.{Period => Duration}
import fr.polytechnique.cmap.cnam.etl.datatypes._
import fr.polytechnique.cmap.cnam.etl.events.{Event, Exposure}


/***
  * Internal Data representation that allows LimitedPeriodAdder to transform DrugPurchase into an Exposure.
  *
  * There is only two ways of creating a Duration Exposure:
  * 1. From transforming a `Event[Drug]`. This transformation is the responsibility of the caller.
  * 2. By calling `+` of this class.
  * @param patientID string representation of patientID
  * @param value equivalent of the value of `Event[Drug]`
  * @param period represents the `Timestamps` of the beginning and the end of the Exposure.
  * @param span long that represents the duration of the Exposure.
  */
case class ExposureDuration(patientID: String, value: String, period: Period, span: Long)
  extends Addable[ExposureDuration] {
  self =>
  /***
    * Add two `ExposureDuration` to form a `RemainingPeriod[ExposureDuration]`. For the exact rules, look at the
    * test of `+` for `Period` & `ExposureDuration`
    * @param other other `ExposureDruration` to be added
    * @return result of the addition as `RemainingPeriod[ExposureDuration]`
    */
  override def +(other: ExposureDuration): RemainingPeriod[ExposureDuration] =
    if ((self.patientID != other.patientID) | (self.value != other.value)) {
      RightRemainingPeriod(self)
    } else {
      self.period + other.period match {
        case RightRemainingPeriod(p) =>
          RightRemainingPeriod(ExposureDuration(self.patientID, self.value, p, self.span + other.span))

        case DisjointedRemainingPeriod(LeftRemainingPeriod(p1), RightRemainingPeriod(p2)) =>
          if (p1 == self.period) {
            DisjointedRemainingPeriod(
              LeftRemainingPeriod(ExposureDuration(self.patientID, self.value, p1, self.span)),
              RightRemainingPeriod(ExposureDuration(other.patientID, other.value, p2, other.span))
            )
          } else {
            DisjointedRemainingPeriod(
              LeftRemainingPeriod(ExposureDuration(other.patientID, other.value, p1, other.span)),
              RightRemainingPeriod(ExposureDuration(self.patientID, self.value, p2, self.span))
            )
          }
        // avoid scala match may not be exhaustive
        case _ => NullRemainingPeriod
      }
    }
}

/***
  * Defines the strategies to be used to add different ExposureDuration and make them one.
  */
sealed trait ExposureDurationStrategy extends Function1[ExposureDuration, Event[Exposure]] with Serializable

/***
  * Sets the Exposure end as start + purchase_1*purchase_1_duration + purchase_2*purchase_2_duration ...
  */
object PurchaseCountBased extends ExposureDurationStrategy {
  override def apply(v1: ExposureDuration): Event[Exposure] = {
    Exposure(v1.patientID, v1.value, 1D, v1.period.start, v1.period.start + Duration(milliseconds = v1.span) get)
  }
}

/***
  * Sets the Exposure end as being the end of the last Exposure Duration end.
  */
object LatestPurchaseBased extends ExposureDurationStrategy {
  override def apply(v1: ExposureDuration): Event[Exposure] = {
    Exposure(v1.patientID, "NA", v1.value, 1D, v1.period.start, Some(v1.period.end))
  }
}
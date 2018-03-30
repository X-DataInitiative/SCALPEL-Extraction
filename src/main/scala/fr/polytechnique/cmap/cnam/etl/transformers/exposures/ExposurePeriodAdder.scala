package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import org.apache.spark.sql.DataFrame
import me.danielpes.spark.datetime.Period
import me.danielpes.spark.datetime.implicits._

trait ExposurePeriodAdder {

  val exposurePeriodStrategy: ExposurePeriodStrategy

  implicit def exposurePeriodImplicits(data: DataFrame): ExposurePeriodAdderImpl = {

    exposurePeriodStrategy match {
      case ExposurePeriodStrategy.Limited => new LimitedExposurePeriodAdder(data)
      case ExposurePeriodStrategy.Unlimited => new UnlimitedExposurePeriodAdder(data)
    }
  }
}

abstract class ExposurePeriodAdderImpl(data: DataFrame) {
  def withStartEnd(
      minPurchases: Int,
      startDelay: Period,
      purchasesWindow: Period,
      endThreshold: Option[Period],
      endDelay: Option[Period])
    : DataFrame
}
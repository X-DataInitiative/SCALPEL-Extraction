package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import org.apache.spark.sql.DataFrame

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
  def withStartEnd(minPurchases: Int, startDelay: Int, endDelay: Int, purchasesWindow: Int, endThreshold: Int = 4): DataFrame
}
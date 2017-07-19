package fr.polytechnique.cmap.cnam.etl.transformers.exposures

sealed trait ExposurePeriodStrategy

object ExposurePeriodStrategy  {
  case object Limited extends ExposurePeriodStrategy
  case object Unlimited extends ExposurePeriodStrategy

  def fromString(value: String): ExposurePeriodStrategy = value.toLowerCase match {
    case "limited" => ExposurePeriodStrategy.Limited
    case "unlimited" => ExposurePeriodStrategy.Unlimited
  }
}
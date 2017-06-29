package fr.polytechnique.cmap.cnam.etl.transformer.exposure


sealed trait WeightAggStrategy


object WeightAggStrategy  {
  case object NonCumulative extends WeightAggStrategy
  case object PurchaseBased extends WeightAggStrategy
  case object DosageBased extends WeightAggStrategy
  case object TimeBased extends WeightAggStrategy

  def fromString(value: String): WeightAggStrategy = value.toLowerCase match {
    case "non-cumulative" | "noncumulative" => WeightAggStrategy.NonCumulative
    case "purchase-based" | "purchasebased" => WeightAggStrategy.PurchaseBased
    case "dosage-based" | "dosagebased" => WeightAggStrategy.DosageBased
    case "time-based" | "timebased" => WeightAggStrategy.TimeBased
  }
}
package fr.polytechnique.cmap.cnam.etl.transformers.follow_up

trait FollowUpTransformerConfig {
  val delayMonths: Int
  val firstTargetDisease: Boolean
  val outcomeName: Option[String]
}

object FollowUpTransformerConfig {
  def apply(delayMonths: Int, firstTargetDisease: Boolean, outcomeName: Option[String]) = {
    val _delayMonths: Int = delayMonths
    val _firstTargetDisease: Boolean = firstTargetDisease
    val _outcomeName: Option[String] = outcomeName

    new FollowUpTransformerConfig {
      val delayMonths: Int = _delayMonths
      val firstTargetDisease: Boolean = _firstTargetDisease
      val outcomeName: Option[String] = _outcomeName
    }
  }
}

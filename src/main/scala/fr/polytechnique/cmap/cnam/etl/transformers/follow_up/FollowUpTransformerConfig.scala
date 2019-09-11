package fr.polytechnique.cmap.cnam.etl.transformers.follow_up

import fr.polytechnique.cmap.cnam.etl.transformers.TransformerConfig

class FollowUpTransformerConfig(
  val delayMonths: Int,
  val firstTargetDisease: Boolean,
  val outcomeName: Option[String]) extends TransformerConfig

object FollowUpTransformerConfig {

  def apply(delayMonths: Int, firstTargetDisease: Boolean, outcomeName: Option[String]) =
    new FollowUpTransformerConfig(delayMonths, firstTargetDisease, outcomeName)
}

// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.molecules

import fr.polytechnique.cmap.cnam.etl.extractors.ExtractorConfig
import fr.polytechnique.cmap.cnam.etl.extractors.codes.ExtractorCodes

/**
  * Base definition of the config needed by the MoleculePurchases extractor.
  * If the MoleculePurchases extractor is needed by a study, it must define either a case class
  * (if mutable) or an object (if hardcoded) extending this class.
  * Important: It cannot be used directly by a study, because it's not compatible with pureconfig.
  */
class MoleculePurchasesConfig(
  val drugClasses: List[String],
  val maxBoxQuantity: Int) extends ExtractorConfig with ExtractorCodes {
  override def isEmpty: Boolean = drugClasses.isEmpty
}

object MoleculePurchasesConfig {

  def apply(
    drugClasses: List[String] = List(),
    maxBoxQuantity: Int = 10): MoleculePurchasesConfig = {

    new MoleculePurchasesConfig(drugClasses, maxBoxQuantity)
  }
}

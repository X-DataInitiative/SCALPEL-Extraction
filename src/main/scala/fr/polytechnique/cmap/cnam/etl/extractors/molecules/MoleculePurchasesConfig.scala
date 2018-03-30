package fr.polytechnique.cmap.cnam.etl.extractors.molecules

import fr.polytechnique.cmap.cnam.etl.extractors.ExtractorConfig

trait MoleculePurchasesConfig extends ExtractorConfig {
  val drugClasses: List[String]
  val maxBoxQuantity: Int
}

object MoleculePurchasesConfig {
  /** @deprecated
    * For backwards compatibility
    */
  def apply(
      drugClasses: List[String] = List(),
      maxBoxQuantity: Int = 10): MoleculePurchasesConfig = {

    val _drugClasses = drugClasses
    val _maxBoxQuantity = maxBoxQuantity

    new MoleculePurchasesConfig {
      val drugClasses: List[String] = _drugClasses
      val maxBoxQuantity: Int = _maxBoxQuantity
    }
  }
}

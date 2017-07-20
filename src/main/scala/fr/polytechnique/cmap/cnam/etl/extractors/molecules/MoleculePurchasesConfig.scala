package fr.polytechnique.cmap.cnam.etl.extractors.molecules

import fr.polytechnique.cmap.cnam.etl.config.CaseClassConfig

case class MoleculePurchasesConfig(
    drugClasses: List[String] = List(),
    maxBoxQuantity: Int = 10)
  extends CaseClassConfig

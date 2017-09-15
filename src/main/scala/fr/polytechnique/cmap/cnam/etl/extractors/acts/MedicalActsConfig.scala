package fr.polytechnique.cmap.cnam.etl.extractors.acts

import fr.polytechnique.cmap.cnam.etl.config.CaseClassConfig

case class MedicalActsConfig(
    dcirCodes: List[String] = List(),
    mcoCIMCodes: List[String] = List(),
    mcoCECodes: List[String] = List(),
    mcoCCAMCodes: List[String] = List())
  extends CaseClassConfig

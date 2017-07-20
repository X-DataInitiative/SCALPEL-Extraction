package fr.polytechnique.cmap.cnam.etl.extractors.acts

import fr.polytechnique.cmap.cnam.etl.config.CaseClassConfig

case class MedicalActsConfig(
    dcirCodes: List[String],
    mcoCIMCodes: List[String],
    mcoCCAMCodes: List[String])
  extends CaseClassConfig

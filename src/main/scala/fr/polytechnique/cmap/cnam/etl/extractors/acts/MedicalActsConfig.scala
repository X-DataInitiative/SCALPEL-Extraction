package fr.polytechnique.cmap.cnam.etl.extractors.acts

import fr.polytechnique.cmap.cnam.etl.config.CaseClassConfig

case class MedicalActsConfig(
    dcirMedicalActCodes: List[String],
    mcoCIM10MedicalActCodes: List[String],
    mcoCCAMMedicalActCodes: List[String])
  extends CaseClassConfig

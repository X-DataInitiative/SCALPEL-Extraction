package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import fr.polytechnique.cmap.cnam.etl.config.CaseClassConfig

case class DiagnosesConfig(
    imbCodes: List[String] = List(),
    dpCodes: List[String] = List(),
    drCodes: List[String] = List(),
    daCodes: List[String] = List())
  extends CaseClassConfig

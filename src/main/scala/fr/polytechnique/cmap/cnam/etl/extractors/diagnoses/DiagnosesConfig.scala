package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import fr.polytechnique.cmap.cnam.etl.config.CaseClassConfig

case class DiagnosesConfig(
    imbDiagnosisCodes: List[String] = List(),
    mainDiagnosisCodes: List[String] = List(),
    linkedDiagnosisCodes: List[String] = List(),
    associatedDiagnosisCodes: List[String] = List())
  extends CaseClassConfig

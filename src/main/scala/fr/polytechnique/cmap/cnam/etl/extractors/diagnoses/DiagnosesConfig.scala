package fr.polytechnique.cmap.cnam.etl.extractors.diagnoses

import fr.polytechnique.cmap.cnam.etl.extractors.ExtractorConfig

/**
  * Abstract definition of the config needed by the Diagnoses extractor.
  * A concrete study config must extend this trait if the Diagnoses extractor is needed by the study
  */
trait DiagnosesConfig extends ExtractorConfig {
  val dpCodes: List[String]
  val drCodes: List[String]
  val daCodes: List[String]
  val imbCodes: List[String]
}

object DiagnosesConfig {
  /** @deprecated
    * For backwards compatibility
    */
  def apply(
      dpCodes: List[String] = List(),
      drCodes: List[String] = List(),
      daCodes: List[String] = List(),
      imbCodes: List[String] = List()): DiagnosesConfig = {

    val _dpCodes = dpCodes
    val _drCodes = drCodes
    val _daCodes = daCodes
    val _imbCodes = imbCodes

    new DiagnosesConfig {
      val dpCodes: List[String] = _dpCodes
      val drCodes: List[String] = _drCodes
      val daCodes: List[String] = _daCodes
      val imbCodes: List[String] = _imbCodes
    }
  }
}

package fr.polytechnique.cmap.cnam.etl.extractors.acts

import fr.polytechnique.cmap.cnam.etl.extractors.ExtractorConfig

/**
  * Abstract definition of the config needed by the MedicalActs extractor.
  * A concrete study config must extend this trait if the MedicalActs extractor is needed by the study
  */
trait MedicalActsConfig extends ExtractorConfig {
  val dcirCodes: List[String]
  val mcoCIMCodes: List[String]
  val mcoCECodes: List[String]
  val mcoCCAMCodes: List[String]
}

object MedicalActsConfig {
  /** @deprecated
    * For backwards compatibility
    */
  def apply(
      dcirCodes: List[String] = List(),
      mcoCIMCodes: List[String] = List(),
      mcoCECodes: List[String] = List(),
      mcoCCAMCodes: List[String] = List()): MedicalActsConfig = {

    val _dcirCodes = dcirCodes
    val _mcoCIMCodes = mcoCIMCodes
    val _mcoCECodes = mcoCECodes
    val _mcoCCAMCodes = mcoCCAMCodes

    new MedicalActsConfig {
      val dcirCodes: List[String] = _dcirCodes
      val mcoCIMCodes: List[String] = _mcoCIMCodes
      val mcoCECodes: List[String] = _mcoCECodes
      val mcoCCAMCodes: List[String] = _mcoCCAMCodes
    }
  }
}

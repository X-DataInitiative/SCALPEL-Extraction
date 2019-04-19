package fr.polytechnique.cmap.cnam.etl.extractors.prestations

import fr.polytechnique.cmap.cnam.etl.extractors.ExtractorConfig

/**
  * Base definition of the config needed by the Prestations extractor.
  * If the Prestations extractor is needed by a study, it must define either a case class
  * (if mutable) or an object (if hardcoded) extending this class.
  * Important: It cannot be used directly by a study, because it's not compatible with pureconfig.
  */
class PrestationsConfig(
  val medicalSpeCodes: List[String],
  val nonMedicalSpeCodes: List[String]) extends ExtractorConfig

object PrestationsConfig {

  def apply(
    medicalSpeCodes: List[String] = List(),
    nonMedicalSpeCodes: List[String] = List()): PrestationsConfig = {
    new PrestationsConfig(medicalSpeCodes, nonMedicalSpeCodes)
  }
}

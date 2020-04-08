// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.diagnoses

import fr.polytechnique.cmap.cnam.etl.extractors.ExtractorConfig

/**
  * Base definition of the config needed by the Diagnoses extractor.
  * If the Diagnoses extractor is needed by a study, it must define either a case class (if mutable)
  * or an object (if hardcoded) extending this class.
  * Important: It cannot be used directly by a study, because it's not compatible with pureconfig.
  */
class DiagnosesConfig(
  val dpCodes: List[String],
  val drCodes: List[String],
  val daCodes: List[String],
  val imbCodes: List[String]) extends ExtractorConfig

object DiagnosesConfig {

  def apply(
    dpCodes: List[String] = List(),
    drCodes: List[String] = List(),
    daCodes: List[String] = List(),
    imbCodes: List[String] = List()): DiagnosesConfig = {

    new DiagnosesConfig(dpCodes, drCodes, daCodes, imbCodes)
  }
}

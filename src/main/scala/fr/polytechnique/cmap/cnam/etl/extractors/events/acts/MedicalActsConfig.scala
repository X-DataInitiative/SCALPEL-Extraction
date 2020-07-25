// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.acts

import fr.polytechnique.cmap.cnam.etl.extractors.ExtractorConfig

/**
  * Base definition of the config needed by the MedicalActs extractor.
  * If the MedicalActs extractor is needed by a study, it must define either a case class
  * (if mutable) or an object (if hardcoded) extending this class.
  * Important: It cannot be used directly by a study, because it's not compatible with pureconfig.
  */
class MedicalActsConfig(
  val dcirCodes: List[String],
  val mcoCIMCodes: List[String],
  val mcoCECodes: List[String],
  val mcoCCAMCodes: List[String],
  val ssrCCAMCodes: List[String],
  val ssrCECodes: List[String],
  val ssrCSARRCodes: List[String],
  val hadCCAMCodes: List[String]
) extends ExtractorConfig

object MedicalActsConfig {

  def apply(
    dcirCodes: List[String] = List(),
    mcoCIMCodes: List[String] = List(),
    mcoCECodes: List[String] = List(),
    mcoCCAMCodes: List[String] = List(),
    ssrCCAMCodes: List[String] = List(),
    ssrCECodes: List[String] = List(),
    ssrCSARRCodes: List[String] = List(),
    hadCCAMCodes: List[String] = List()): MedicalActsConfig = {

    new MedicalActsConfig(
      dcirCodes,
      mcoCIMCodes,
      mcoCECodes,
      mcoCCAMCodes,
      ssrCSARRCodes,
      ssrCCAMCodes,
      ssrCECodes,
      hadCCAMCodes
    )
  }
}

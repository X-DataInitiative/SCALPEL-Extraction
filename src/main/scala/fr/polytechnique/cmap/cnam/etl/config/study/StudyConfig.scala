package fr.polytechnique.cmap.cnam.etl.config.study

import fr.polytechnique.cmap.cnam.etl.config.Config

trait StudyConfig extends Config

object StudyConfig {

  case class InputPaths(
    dcir: Option[String] = None,
    mco: Option[String] = None,
    mcoCe: Option[String] = None,
    had: Option[String] = None,
    ssr: Option[String] = None,
    irBen: Option[String] = None,
    irImb: Option[String] = None,
    irPha: Option[String] = None,
    dosages: Option[String] = None)

  case class OutputPaths(
    override val root: String, override val saveMode: String = "errorIfExists") extends Config.OutputPaths(root, saveMode)

}

package fr.polytechnique.cmap.cnam.etl.config

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

  case class  OutputPaths(
    root: String,
    patients: String,
    flatEvents: String,
    coxFeatures: String,
    ltsccsFeatures: String,
    mlppFeatures: String,
    outcomes: String,
    exposures: String)
}

trait StudyConfig

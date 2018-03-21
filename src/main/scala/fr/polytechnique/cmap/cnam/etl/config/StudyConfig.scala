package fr.polytechnique.cmap.cnam.etl.config

object StudyConfig {
  case class InputPaths(
     dcir: String,
     pmsiMco: String,
     pmsiHad: String,
     pmsiSsr: String,
     irBen: String,
     irImb: String,
     irPha: String,
     dosages: String
   )

  case class  OutputPaths(
     root: String,
     patients: String,
     flatEvents: String,
     coxFeatures: String,
     ltsccsFeatures: String,
     mlppFeatures: String,
     outcomes: String,
     exposures: String
   )
}

trait StudyConfig {
  val inputPaths: StudyConfig.InputPaths
  val outputPaths: StudyConfig.OutputPaths
}

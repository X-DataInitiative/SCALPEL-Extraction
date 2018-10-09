package fr.polytechnique.cmap.cnam.etl.config.mlpp

import fr.polytechnique.cmap.cnam.etl.config.Config

trait MLPPLoaderConfig extends Config

object MLPPLoaderConfig {

  case class InputPaths(
    patients: Option[String] = None,
    outcomes: Option[String] = None,
    exposures: Option[String] = None)

  case class OutputPaths(
    override val root: String,
    override val saveMode: String = "errorIfExists") extends Config.OutputPaths(root, saveMode) {

    //the paths as below are related to the root
    lazy val outcomes: String = s"$outputSavePath/csv/Outcomes.csv"
    lazy val staticStaticOutcomes: String = s"$outputSavePath/csv/StaticOutcomes.csv"
    lazy val outcomesLookup: String = s"$outputSavePath/csv/OutcomesLookup.csv"
    lazy val staticExposuresParquet: String = s"$outputSavePath/parquet/StaticExposures"
    lazy val staticExposuresCSV: String = s"$outputSavePath/csv/StaticExposures.csv"
    lazy val sparseFeaturesParquet: String = s"$outputSavePath/parquet/SparseFeatures"
    lazy val sparseFeaturesCSV: String = s"$outputSavePath/csv/SparseFeatures.csv"
    lazy val moleculeLookup: String = s"$outputSavePath/csv/MoleculeLookup.csv"
    lazy val patientsLookup: String = s"$outputSavePath/csv/PatientsLookup.csv"
    lazy val metadata: String = s"$outputSavePath/csv/metadata.csv"
    lazy val censoring: String = s"$outputSavePath/csv/Censoring.csv"
  }

}

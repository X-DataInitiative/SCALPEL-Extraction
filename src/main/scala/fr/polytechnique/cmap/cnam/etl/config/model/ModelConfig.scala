package fr.polytechnique.cmap.cnam.etl.config.model

import fr.polytechnique.cmap.cnam.etl.config.Config
import fr.polytechnique.cmap.cnam.util.Path

trait ModelConfig extends Config

object ModelConfig {

  abstract class InputPaths(
    patients: Option[String] = None,
    outcomes: Option[String] = None,
    exposures: Option[String] = None)

  abstract class OutputPaths(root: Path) //the root may be different in the different case

}

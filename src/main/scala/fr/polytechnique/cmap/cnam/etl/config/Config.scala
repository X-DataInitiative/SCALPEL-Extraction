package fr.polytechnique.cmap.cnam.etl.config

import fr.polytechnique.cmap.cnam.util._

trait Config

object Config {

  abstract class OutputPaths(
    val root: String, val saveMode: String = "errorIfExists") {

    // used to add a ts at the end of the root when the save_mode = "new"
    lazy val outputSavePath: String = {
      if (saveMode == "withTimestamp") {
        Path(root).withTimestampSuffix().toString
      } else {
        root.toString
      }
    }
  }
}

package fr.polytechnique.cmap.cnam.etl.config.model

import java.sql.Timestamp
import pureconfig.ConfigReader
import fr.polytechnique.cmap.cnam.etl.config.ConfigLoader
import fr.polytechnique.cmap.cnam.util.Path
import fr.polytechnique.cmap.cnam.util.functions.{makeTS, parseTimestamp}

trait ModelConfigLoader extends ConfigLoader {
  //convert yyyy-MM-dd to Timestamp
  implicit val timeStampReader: ConfigReader[Timestamp] = ConfigReader[String].map(
    str => parseTimestamp(str, "yyyy-MM-dd").getOrElse(makeTS(2006, 1, 1)))

  //read the path
  implicit val pathReader: ConfigReader[Path] = ConfigReader[String].map(Path(_))
}

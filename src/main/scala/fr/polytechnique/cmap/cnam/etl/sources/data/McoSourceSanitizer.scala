package fr.polytechnique.cmap.cnam.etl.sources.data

import org.apache.spark.sql.DataFrame

trait McoSourceSanitizer {

  implicit def addMcoSourceSanitizerImplicits(rawMco: DataFrame) : McoFilters = {
    new McoFilters(rawMco)
  }
}

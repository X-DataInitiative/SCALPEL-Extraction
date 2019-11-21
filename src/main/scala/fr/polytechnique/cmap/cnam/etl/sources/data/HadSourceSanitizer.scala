package fr.polytechnique.cmap.cnam.etl.sources.data

import org.apache.spark.sql.DataFrame

trait HadSourceSanitizer {

  implicit def addhadSourceSanitizerImplicits(rawHad: DataFrame): HadFilters = {
    new HadFilters(rawHad)
  }
}

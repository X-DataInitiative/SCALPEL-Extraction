package fr.polytechnique.cmap.cnam.etl.sources.data

import org.apache.spark.sql.DataFrame

trait SsrSourceSanitizer {

  implicit def addSsrSourceSanitizerImplicits(rawSsr: DataFrame): SsrFilters = {
    new SsrFilters(rawSsr)
  }
}

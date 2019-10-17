// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.sources.data

import org.apache.spark.sql.DataFrame

trait DcirSourceSanitizer {
  implicit def addDcirSourceSantizerImplicits(rawDcir: DataFrame): DcirFilters = new DcirFilters(rawDcir)
}

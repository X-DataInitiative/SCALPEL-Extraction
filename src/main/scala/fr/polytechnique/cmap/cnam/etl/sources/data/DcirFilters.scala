// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.sources.data

import org.apache.spark.sql.DataFrame

private[data] class DcirFilters(rawDcir: DataFrame) {
  def filterInformationFlux: DataFrame = {
    rawDcir.where(DcirSource.DPN_QLF =!= 71)
  }
}

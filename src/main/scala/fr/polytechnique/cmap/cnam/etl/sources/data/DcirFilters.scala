// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.sources.data

import org.apache.spark.sql.DataFrame

private[data] class DcirFilters(rawDcir: DataFrame) {
  /** Remove lines for information only.
    *
    * @return dataframe with lines corresponding to some real interaction with the healthcare system.
    */
  def filterInformationFlux: DataFrame = {
    rawDcir.where(DcirSource.DPN_QLF =!= 71)
  }
}

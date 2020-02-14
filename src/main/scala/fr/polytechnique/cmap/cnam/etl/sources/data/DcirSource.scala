package fr.polytechnique.cmap.cnam.etl.sources.data

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}

object DcirSource extends DataSourceManager with DcirSourceSanitizer {

  /** BSE_PRS_NAT: Nature de la prestation (acte de base) */
  val BSE_PRS_NAT: Column = col("BSE_PRS_NAT")
  val BEN_CDI_NIR: Column = col("BEN_CDI_NIR")
  val DPN_QLF: Column = col("DPN_QLF")

  /** Sanitize the dcir with usual filter for analysis
    * - remove the lines without a proper *Nature de la prestation*
    * - remove the lines for information
    * @param dcir
    * @return a new instance of the Source, with the sanitized data
    */
  override def sanitize(dcir: DataFrame): DataFrame = {
    dcir.where(DcirSource.BSE_PRS_NAT =!= 0)
      .filterInformationFlux
  }
}

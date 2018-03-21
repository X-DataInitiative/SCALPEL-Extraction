package fr.polytechnique.cmap.cnam.etl.sources.data

import fr.polytechnique.cmap.cnam.etl.sources.SourceManager
import org.apache.spark.sql.DataFrame

/**
  * Extractor class for the MCO table
  * This filtering is explained here
  * https://datainitiative.atlassian.net/wiki/pages/viewpage.action?pageId=40304642
  */
object McoSource extends SourceManager with McoSourceSanitizer{

  override def sanitize(rawMco: DataFrame): DataFrame = {
    /**
      * This filtering is explained here
      * https://datainitiative.atlassian.net/wiki/pages/viewpage.action?pageId=40304642
      */
    rawMco
      .filterSpecialHospitals
      .filterSharedHospitalStays
      .filterIVG
      .filterNonReimbursedStays
      .filterMcoCorruptedHospitalStays
  }
}

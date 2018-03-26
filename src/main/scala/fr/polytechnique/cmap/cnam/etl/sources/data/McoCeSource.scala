package fr.polytechnique.cmap.cnam.etl.sources.data

import fr.polytechnique.cmap.cnam.etl.sources.SourceManager
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}

object McoCeSource extends SourceManager with McoSourceSanitizer{

  val IAS_RET: Column = col("IAS_RET")
  val ENT_DAT_RET: Column = col("ENT_DAT_RET")

  override def sanitize(mcoCe: DataFrame): DataFrame = {
    mcoCe
      .filterSpecialHospitals
      .filterMcoCeCorruptedHospitalStays
  }
}

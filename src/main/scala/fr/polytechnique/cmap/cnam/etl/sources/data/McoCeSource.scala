package fr.polytechnique.cmap.cnam.etl.sources.data

import fr.polytechnique.cmap.cnam.etl.sources.SourceManager
import org.apache.spark.sql.{DataFrame, SQLContext}

object McoCeSource extends SourceManager with McoSourceSanitizer{

  override def read(sqlContext: SQLContext, path: String): DataFrame = McoSource.read(sqlContext, path)

  override def sanitize(mcoCe: DataFrame): DataFrame = {
    mcoCe
      .filterSpecialHospitals
      .filterMcoCeCorruptedHospitalStays
  }
}



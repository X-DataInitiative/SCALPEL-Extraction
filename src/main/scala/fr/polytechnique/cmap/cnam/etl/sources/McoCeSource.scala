package fr.polytechnique.cmap.cnam.etl.sources

import org.apache.spark.sql.{DataFrame, SQLContext}

object McoCeSource extends SourceManager {

  override def read(sqlContext: SQLContext, path: String): DataFrame = McoSource.read(sqlContext, path)
  override def sanitize(mcoCe: DataFrame): DataFrame = McoSource.sanitize(mcoCe)
}



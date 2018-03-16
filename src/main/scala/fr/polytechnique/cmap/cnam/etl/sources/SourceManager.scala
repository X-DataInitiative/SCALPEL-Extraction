package fr.polytechnique.cmap.cnam.etl.sources

import org.apache.spark.sql.{DataFrame, SQLContext}

trait SourceManager extends SourceReader with SourceSanitizer {

  /**
    * @deprecated Soon to be removed.
    */
  def readAndSanitize(sqlContext: SQLContext, path: String): DataFrame = {
    sanitize(read(sqlContext, path))
  }
}

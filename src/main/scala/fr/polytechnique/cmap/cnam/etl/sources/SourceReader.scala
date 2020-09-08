// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.sources

import org.apache.spark.sql.{DataFrame, SQLContext}

private[sources] trait SourceReader {
  self: SourceManager =>
  def read(sqlContext: SQLContext, path: String, fileFormat: String = "parquet"): DataFrame = {
    fileFormat match {
      case "orc" => sqlContext.read.orc(path)
      case "parquet" =>  sqlContext.read.parquet(path)
      case _ => sqlContext.read.parquet(path)
    }
  }
}

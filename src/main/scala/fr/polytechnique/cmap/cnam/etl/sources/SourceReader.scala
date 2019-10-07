// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.sources

import org.apache.spark.sql.{DataFrame, SQLContext}

private[sources] trait SourceReader {
  self: SourceManager =>
  def read(sqlContext: SQLContext, path: String): DataFrame = sqlContext.read.parquet(path)
}

package fr.polytechnique.cmap.cnam.etl.sources

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SQLContext}

private[sources] object Dcir extends SourceReader {

  override def read(sqlContext: SQLContext, dcirPath: String): DataFrame = {
    super.read(sqlContext, dcirPath).where(col("BSE_PRS_NAT") !== 0)
  }
}

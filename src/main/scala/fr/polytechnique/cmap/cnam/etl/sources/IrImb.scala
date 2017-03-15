package fr.polytechnique.cmap.cnam.etl.sources

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SQLContext}

private[sources] object IrImb extends SourceReader
{
  override def read(sqlContext: SQLContext, irImbPath: String): DataFrame = {
    super.read(sqlContext, irImbPath)
      //.where(col("IMB_ALD_DTD") isNotNull)
      .where(col("IMB_ALD_DTD") =!= "")
  }

}

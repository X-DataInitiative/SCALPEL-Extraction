package fr.polytechnique.cmap.cnam.etl.sources

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}

private[sources] object IrImbSource extends SourceManager {

  val IMB_ALD_DTD: Column = col("IMB_ALD_DTD")

  override def sanitize(irImb: DataFrame): DataFrame = {
    irImb.where(IrImbSource.IMB_ALD_DTD =!= "")
  }
}

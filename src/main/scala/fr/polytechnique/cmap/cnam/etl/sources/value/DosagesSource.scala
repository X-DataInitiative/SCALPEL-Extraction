// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.sources.value

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import fr.polytechnique.cmap.cnam.etl.sources.SourceManager

object DosagesSource extends SourceManager {

  val MoleculeName: Column = col("MOLECULE_NAME")

  override def read(sqlContext: SQLContext, path: String, fileFormat: String = "parquet"): DataFrame = {
    sqlContext.read
      .format("csv")
      .option("header", "true")
      .load(path)
      .select(
        col("PHA_PRS_IDE"),
        col("MOLECULE_NAME"),
        col("TOTAL_MG_PER_UNIT").cast(IntegerType)
      )
  }

  override def sanitize(dosages: DataFrame): DataFrame = {
    dosages.where(DosagesSource.MoleculeName =!= "BENFLUOREX")
  }
}

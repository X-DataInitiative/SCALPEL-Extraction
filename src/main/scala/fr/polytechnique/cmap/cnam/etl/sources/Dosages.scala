package fr.polytechnique.cmap.cnam.etl.sources

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SQLContext}

private[sources] object Dosages extends SourceReader() {

  override def read(sqlContext: SQLContext, path: String): DataFrame = {
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(path)
      .select(
        col("PHA_PRS_IDE"),
        col("MOLECULE_NAME"),
        col("TOTAL_MG_PER_UNIT").cast(IntegerType)
      )
      .where(col("MOLECULE_NAME") !== "BENFLUOREX")
  }
}

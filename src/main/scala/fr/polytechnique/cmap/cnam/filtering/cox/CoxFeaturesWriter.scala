package fr.polytechnique.cmap.cnam.filtering.cox

import org.apache.spark.sql.Dataset

object CoxFeaturesWriter {

  implicit class CoxFeatures(data: Dataset[CoxFeature]) {

    def writeParquet(path: String): Unit = data.toDF.write.parquet(path)
    def writeCSV(path: String): Unit = {
      data.toDF.orderBy("patientID", "start")
        .coalesce(1)
        .write
        .format("com.databricks.spark.csv")
        .option("delimiter", ",")
        .option("header", "true")
        .save(path)
    }
  }
}

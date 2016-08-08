package fr.polytechnique.cmap.cnam.utilities

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * Created by burq on 30/06/16.
  */
object RichDataFrames {

  implicit class CSVDataFrame(dataFrame: DataFrame) {

    def writeParquet(path: String, isOverwrite: Boolean = true ): Unit = {
      val saveMode : SaveMode = if(isOverwrite) SaveMode.Ignore else SaveMode.Overwrite
      dataFrame.write
        .mode(saveMode)
        .parquet(path)
    }

    def ===(other: DataFrame): Boolean = dataFrame.except(other).count == 0

  }
}

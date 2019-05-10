package fr.polytechnique.cmap.cnam.etl.sources.data

import java.sql.Timestamp
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}
import fr.polytechnique.cmap.cnam.etl.sources.SourceManager

trait DataSourceManager extends SourceManager {

  val EXE_SOI_DTD: Column = col("EXE_SOI_DTD")

  /**
    * This method santize the sources based on the passed dates.
    *
    * @param sourceData the data source that will be sanitized
    * @param studyStart the study start date
    * @param studyEnd   the study end date
    * @return a Dataframe with lines that respect the condition
    */
  def sanitizeDates(sourceData: DataFrame, studyStart: Timestamp, studyEnd: Timestamp): DataFrame = {
    sourceData.where((EXE_SOI_DTD >= studyStart) && (EXE_SOI_DTD <= studyEnd))
  }
}

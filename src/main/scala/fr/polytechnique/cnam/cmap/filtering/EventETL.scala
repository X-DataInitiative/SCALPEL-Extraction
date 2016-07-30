package fr.polytechnique.cnam.cmap.filtering

import org.apache.spark.sql.{DataFrame, Dataset}
import com.typesafe.config.Config

/**
  * @author Daniel de Paula
  */
abstract class EventETL(config: Config, patients: Dataset[Patient]) {

  protected def extract(paths: String): DataFrame

  protected def transform(rawData: DataFrame): Dataset[Event]

  protected def load(events: Dataset[Event], paths: String)

  def run(): Unit = {
    val inPaths = config.getString("input.paths")
    val outPaths = config.getString("output.paths")
    val rawData: DataFrame = this.extract(inPaths)
    val transformedData = this.transform(rawData)
    this.load(transformedData, outPaths)
  }
}

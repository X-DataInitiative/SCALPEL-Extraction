// License: BSD 3 clause

package fr.polytechnique.cmap.cnam

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}
import fr.polytechnique.cmap.cnam.util.{Locales, LoggerLevels}

trait Main extends LoggerLevels with Locales {

  @transient final lazy val logger: Logger = Logger.getLogger(getClass)
  logger.setLevel(Level.INFO)

  @transient private var _spark: SparkSession = _

  def sc: SparkContext = _spark.sparkContext

  // Expected args are in format "arg1=value1 arg2=value2 ..."
  def main(args: Array[String]): Unit = {
    startContext()
    val sqlCtx = sqlContext
    val argsMap = args.map(
      arg => arg.split("=")(0) -> arg.split("=")(1)
    ).toMap
    try {
      run(sqlCtx, argsMap)
    }
    finally stopContext()
  }

  def sqlContext: SQLContext = _spark.sqlContext

  def startContext(): Unit = {
    if (_spark == null) {
      _spark = SparkSession
        .builder()
        .appName(this.appName)
        .config("spark.sql.autoBroadcastJoinThreshold", "104857600")
        .getOrCreate()
    }
  }

  def stopContext(): Unit = {
    if (_spark != null) {
      _spark.stop()
      _spark = null
    }
  }

  def appName: String

  def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]]
}

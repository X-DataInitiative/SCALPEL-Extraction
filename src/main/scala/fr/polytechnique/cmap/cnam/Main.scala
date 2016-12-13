package fr.polytechnique.cmap.cnam

import java.util.{Locale, TimeZone}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}

trait Main {

  Logger.getRootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("fr.polytechnique").setLevel(Level.INFO)

  Locale.setDefault(Locale.US)
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  @transient final lazy val logger: Logger = Logger.getLogger(getClass)
  logger.setLevel(Level.INFO)

  @transient private var _spark: SparkSession = _

  def sc: SparkContext = _spark.sparkContext
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

  def appName: String
  def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]]
}

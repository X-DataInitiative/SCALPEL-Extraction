package fr.polytechnique.cmap.cnam

import java.util.{Locale, TimeZone}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

trait Main {

  Logger.getRootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("fr.polytechnique").setLevel(Level.INFO)

  Locale.setDefault(Locale.US)
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  @transient final lazy val logger = Logger.getLogger(getClass)
  logger.setLevel(Level.INFO)

  @transient private var _sc: SparkContext = _
  @transient private var _sql: HiveContext = _

  def sc: SparkContext = _sc
  def sqlContext: HiveContext = _sql
  def startContext(): Unit = {
    _sc = new SparkContext(new SparkConf().setAppName(this.appName))
    _sql = new HiveContext(_sc)
    _sql.setConf("spark.sql.autoBroadcastJoinThreshold", "104857600")
  }
  def stopContext(): Unit = _sc.stop()

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
  def run(sqlContext: HiveContext, argsMap: Map[String, String]): Option[Dataset[_]]
}

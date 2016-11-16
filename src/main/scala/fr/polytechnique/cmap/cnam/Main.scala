package fr.polytechnique.cmap.cnam

import java.util.{Locale, TimeZone}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

trait Main {

  Logger.getRootLogger.setLevel(Level.WARN)
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("fr.polytechnique").setLevel(Level.WARN)

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
  }
  def stopContext(): Unit = _sc.stop()

  def appName: String
  def run(sqlContext: HiveContext, args: Array[String]): Unit

  final def main(args: Array[String]): Unit = {
    startContext()
    val sqlCtx = sqlContext
    try {
      run(sqlCtx, args)
    }
    finally stopContext()
  }
}

package fr.polytechnique.cmap.cnam

import java.util.{Locale, TimeZone}

import fr.polytechnique.cmap.cnam.filtering.FilteringMain._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

trait Main {

  Logger.getRootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  Locale.setDefault(Locale.US)
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  @transient final lazy val logger = Logger.getLogger(getClass)
  logger.setLevel(Level.INFO)

  @transient private var _sc: SparkContext = _
  @transient private var _sql: HiveContext = _

  def sc: SparkContext = _sc
  def sqlContext: HiveContext = _sql

  def initContexts(): Unit = {
    _sc = new SparkContext(new SparkConf().setAppName(this.appName))
    _sql = new HiveContext(_sc)
  }
  def appName: String
  def main(args: Array[String]): Unit
}

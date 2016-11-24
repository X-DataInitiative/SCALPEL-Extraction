package fr.polytechnique.cmap.cnam

import java.io.File
import java.util.{Locale, TimeZone}
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.scalatest._

abstract class SharedContext extends FlatSpecLike with BeforeAndAfterAll with BeforeAndAfterEach
    with TestHiveSingleton { self: Suite =>

  Logger.getRootLogger.setLevel(Level.WARN)
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("/executors").setLevel(Level.FATAL)

  Locale.setDefault(Locale.US)
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  override val sqlContext: HiveContext = hiveContext
  lazy val sc: SparkContext = sqlContext.sparkContext

  override def beforeEach(): Unit = {
    FileUtils.deleteDirectory(new File("target/test/output"))
    super.beforeEach()
  }

  override def afterAll() {
    FileUtils.deleteDirectory(new File("target/test/output"))
    super.afterAll()
  }
}
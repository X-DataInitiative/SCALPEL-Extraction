package fr.polytechnique.cmap.cnam

import org.apache.spark._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.hive.test.TestHiveContext
import org.scalatest._
/**
  * Created by burq on 05/07/16.
  */
abstract class SharedContext extends FlatSpecLike with BeforeAndAfterAll with BeforeAndAfterEach {
  self: Suite =>

  Logger.getRootLogger.setLevel(Level.WARN)
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("/executors").setLevel(Level.FATAL)

  val conf = new SparkConf().setMaster("local").setAppName("test")

  @transient private var _sc: SparkContext = _

  // We use hiveContext instead of sqlContext because it is more complete (e.g. Window functions).
  // The tradeoff is the overhead of the hive package.
  @transient private var _sql: TestHiveContext = _

  def sc: SparkContext = _sc
  def sqlContext: TestHiveContext = _sql

  override def beforeAll() {
    _sc = new SparkContext(conf)
    _sql = new TestHiveContext(_sc)
    super.beforeAll()
  }

  override def afterAll() {
    try{
      _sc.stop()
      _sc = null
      System.clearProperty("spark.driver.port")
    } finally {
      super.afterAll()
    }
  }
}
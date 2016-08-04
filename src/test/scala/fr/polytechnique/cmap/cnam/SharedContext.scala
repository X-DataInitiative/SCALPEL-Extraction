package fr.polytechnique.cmap.cnam

import org.scalatest._
import org.apache.spark._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SQLContext
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

  @transient private val _sc: SparkContext = SparkContext.getOrCreate(conf)
  @transient private val _sql: SQLContext = new SQLContext(_sc)

  def sc: SparkContext = _sc
  def sqlContext: SQLContext = _sql


  override def beforeAll() {
//    _sc = new SparkContext(conf)
//    _sql = new SQLContext(_sc)
    super.beforeAll()
  }

  override def afterAll() {
    try{
      // While running multiple TestSuits in parallel,
      // stoping _sc here will throw an error.
//      _sc.stop()
//      _sc = null
      System.clearProperty("spark.driver.port")
    } finally {
      super.afterAll()
    }

  }
}
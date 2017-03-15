package fr.polytechnique.cmap.cnam

import java.io.File
import java.util.{Locale, TimeZone}
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.scalatest._
import fr.polytechnique.cmap.cnam.util.RichDataFrames

abstract class SharedContext extends FlatSpecLike with BeforeAndAfterAll with BeforeAndAfterEach {
    self: Suite =>

  Logger.getRootLogger.setLevel(Level.WARN)
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("/executors").setLevel(Level.FATAL)

  Locale.setDefault(Locale.US)
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  private var _spark: SparkSession = _
  protected val debug: Boolean = false

  protected def spark: SparkSession = _spark
  protected def sqlContext: SQLContext = _spark.sqlContext
  protected def sc = _spark.sparkContext


  def assertDFs(df1: DataFrame, df2: DataFrame, debug: Boolean = this.debug): Unit = {
    try {

      df1.persist()
      df2.persist()

      if(debug) {
        df1.printSchema()
        df2.printSchema()
        df1.show(100, false)
        df2.show(100, false)
      }

      import RichDataFrames._
      assert(df1 === df2)

    } finally {
      df1.unpersist()
      df2.unpersist()
    }
  }

  protected override def beforeAll(): Unit = {
    if (_spark == null) {
      _spark = SparkSession
        .builder()
        .appName("Tests")
        .master("local[*]")
        .config("spark.default.parallelism", 2)
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.sql.testkey", "true")
        .getOrCreate()
    }
    // Ensure we have initialized the context before calling parent code
    super.beforeAll()
  }

    override def beforeEach(): Unit = {
      FileUtils.deleteDirectory(new File("target/test/output"))
      super.beforeEach()
    }

    override def afterAll() {
      try {
        if (_spark != null) {
          _spark.stop()
          _spark = null
        }
      } finally {
        FileUtils.deleteDirectory(new File("target/test/output"))
        super.afterAll()
      }
    }
}
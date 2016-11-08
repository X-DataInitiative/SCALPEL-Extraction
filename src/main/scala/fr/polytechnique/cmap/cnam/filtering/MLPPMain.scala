package fr.polytechnique.cmap.cnam.filtering

import scala.collection.JavaConversions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import com.typesafe.config.{Config, ConfigFactory}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.utilities.functions._
/**
  * Created by sathiya on 18/10/16.
  */
object MLPPMain extends Main {

  override def appName: String = "MLPPFeaturing"

  def MLPPFeaturing(sqlContext: HiveContext, config: Config): Unit = {
    import sqlContext.implicits._

    val bucketSize = config.getString("hypothesis.bucketSize")
    val lagCount = config.getInt("hypothesis.lagCount")
    val minTimestampList = config.getIntList("hypothesis.minTimeStamp")
    val maxTimestampList = config.getIntList("hypothesis.maxTimeStamp")
    val outputRootDir = config.getString("paths.output")
    val inputRootDir = config.getString("paths.input")

    val flatEvents = sqlContext.read.parquet(config.getString("paths.input.events"))
      .where(col("category").isin("disease", "mlpp_exposure"))
      .withColumn("category",
        when(col("category") === "mlpp_exposure", lit("exposure"))
          .otherwise(col("category")))
      .as[FlatEvent]
      .persist()

//    val params = MLPPWriter.Params(
//      bucketSize = bucketSize,
//      lagCount = lagCount,
//      minTimestamp = makeTS(minTimestampList.toList),
//      maxTimestamp = makeTS(maxTimestampList.toList)
//    )
//    val writer = MLPPWriter(params)
//    val result = writer.write(flatEvents, outputRootDir)
  }

  def main(args: Array[String]): Unit = {
    startContext()
    val environment = if (args.nonEmpty) args(0) else "test"
    val config: Config = ConfigFactory.parseResources("mlpp.conf").getConfig(environment)
    MLPPFeaturing(sqlContext, config)
    stopContext()
  }
}
package fr.polytechnique.cmap.cnam.filtering.mlpp

import scala.collection.JavaConversions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import com.typesafe.config.{Config, ConfigFactory}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.filtering.FlatEvent
import fr.polytechnique.cmap.cnam.utilities.functions._
/**
  * Created by sathiya on 18/10/16.
  */
object MLPPMain extends Main {

  override def appName: String = "MLPPFeaturing"

  def MLPPFeaturing(sqlContext: HiveContext, config: Config): Unit = {
    import sqlContext.implicits._

    val bucketSize = config.getInt("hypothesis.bucketSize")
    val lagCount = config.getInt("hypothesis.lagCount")
    val minTimestampList = config.getIntList("hypothesis.minTimeStamp")
    val maxTimestampList = config.getIntList("hypothesis.maxTimeStamp")
    val outputRootDir = config.getString("paths.output.root")
    val inputFlatEvents = config.getString("paths.input.flatEvents")

    val flatEvents = sqlContext.read.parquet(config.getString(inputFlatEvents))
      .where(col("category").isin("disease", "mlpp_exposure"))
      .withColumn("category",
        when(col("category") === "mlpp_exposure", lit("exposure"))
          .otherwise(col("category")))
      .as[FlatEvent]
      .persist()

    val mlppParams = MLPPWriter.Params(
      bucketSize = bucketSize,
      lagCount = lagCount,
      minTimestamp = makeTS(minTimestampList.toList),
      maxTimestamp = makeTS(maxTimestampList.toList)
    )
    val mlppWriter = MLPPWriter(mlppParams)
    mlppWriter.write(flatEvents, outputRootDir)
  }

  def main(args: Array[String]): Unit = {
    startContext()
    val environment = if (args.nonEmpty) args(0) else "test"
    val config: Config = ConfigFactory.parseResources("mlpp.conf").getConfig(environment)
    MLPPFeaturing(sqlContext, config)
    stopContext()
  }
}
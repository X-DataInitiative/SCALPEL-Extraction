package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.hive.HiveContext
import com.typesafe.config.{Config, ConfigFactory}
import fr.polytechnique.cmap.cnam.Main
/**
  * Created by sathiya on 18/10/16.
  */
object MLPPMain extends Main {

  override def appName: String = "MLPP"

  def runMLPP(sqlContext: HiveContext, config: Config): Unit = {
    import sqlContext.implicits._

    val bucketSize = config.getString("hypothesis.bucketSize")
    val lagCount = config.getInt("hypothesis.lagCount")
    val minTimestampList = config.getIntList("hypothesis.minTimeStamp")
    val maxTimestampList = config.getIntList("hypothesis.maxTimeStamp")
    val outputRootDir = config.getString("paths.output")
    val inputRootDir = config.getString("paths.input")

    val flatEvents = sqlContext.read.parquet(config.getString("paths.input.events"))
      .as[FlatEvent]
      .persist()

//    val params = MLPPWriter.Params(
//      bucketSize = bucketSize,
//      lagCount = lagCount,
//      minTimestamp = makeTS(
//        minTimestampList.get(0),
//        minTimestampList.get(1),
//        minTimestampList.get(2)),
//      maxTimestamp = makeTS(
//        maxTimestampList.get(0),
//        maxTimestampList.get(1),
//        maxTimestampList.get(2),
//        maxTimestampList.get(3),
//        maxTimestampList.get(4),
//        maxTimestampList.get(5)),
//    )
//    val writer = MLPPWriter(params)
//    val result = writer.write(flatEvents, outputRootDir)
  }

  def main(args: Array[String]): Unit = {
    startContext()
    val environment = if (args.nonEmpty) args(0) else "test"
    val config: Config = ConfigFactory.parseResources("mlpp.conf").getConfig(environment)
    runMLPP(sqlContext, config)
    stopContext()
  }
}
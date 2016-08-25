package fr.polytechnique.cmap.cnam.filtering

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.Dataset
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  Logger.getRootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  def runETL(sqlContext: HiveContext, config: Config): Unit = {
    import implicits._
    val sources: Sources = sqlContext.extractAll(config.getConfig("paths.input"))

    val patients: Dataset[Patient] = PatientsTransformer.transform(sources)
    val drugEvents: Dataset[Event] = DrugEventsTransformer.transform(sources)
    val cancerEvents: Dataset[Event] = CancerTransformer.transform(sources)

    val events = drugEvents.union(cancerEvents)

    patients.toDF.write.parquet(config.getString("paths.output.patients"))
    events.writeFlatEvent(patients, config.getString("paths.output.events"))
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Filtering")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val environment = if (args.nonEmpty) args(0) else "test"
    val config: Config = ConfigFactory.parseResources("filtering.conf").getConfig(environment)
    runETL(sqlContext, config)
  }
}

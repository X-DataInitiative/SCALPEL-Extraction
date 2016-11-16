package fr.polytechnique.cmap.cnam.filtering.mlpp

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import com.typesafe.config.{Config, ConfigFactory}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.filtering.FlatEvent

// Used to run the code @CNAM on 09/11/2016 (Donald Trump's election day)
object MLPPProvisoryMain extends Main {

  override def appName: String = "MLPPMain"

  def runMLPPFeaturing(sqlContext: SQLContext, config: Config, params: MLPPWriter.Params) = {
    import sqlContext.implicits._

    Seq("broad", "narrow").foreach { i =>
      val coxPatients = sqlContext.read.parquet(s"/shared/burq/filtered_data/$i/cox").select("patientID").distinct

      val flatEventsDF = sqlContext.read.parquet(s"/shared/burq/filtered_data/$i/events")
        .where(col("category").isin("trackloss", "disease", "molecule"))
        .join(coxPatients, "patientID")
        .withColumn("category", when(col("category") === "molecule", lit("exposure")).otherwise(col("category")))

      val flatEvents = flatEventsDF.as[FlatEvent].persist

      MLPPWriter(params).write(flatEvents, s"/shared/mlpp_features/$i/")
    }
  }

  override def main(args: Array[String]): Unit = {
    startContext()
    val environment = args(0)
    val params = MLPPWriter.Params(lagCount = args(1).toInt, bucketSize = args(2).toInt)
    val config: Config = ConfigFactory.parseResources("filtering.conf").getConfig(environment)
    runMLPPFeaturing(sqlContext, config, params)
    stopContext()
  }
}

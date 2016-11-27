package fr.polytechnique.cmap.cnam.filtering.mlpp

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.filtering._

object MLPPMain extends Main {

  override def appName: String = "MLPPFeaturing"

  def run(sqlContext: HiveContext, argsMap: Map[String, String] = Map()): Option[Dataset[MLPPFeature]] = {

    import sqlContext.implicits._

    // "get" returns an Option, then we can use foreach to gently ignore when the key was not found.
    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))

    val outputPath: String = FilteringConfig.outputPaths.mlppFeatures
    val flatEvents: Dataset[FlatEvent] = FilteringMain.run(sqlContext).get
      .filter(e => e.category == "molecule" || e.category == "disease").cache()

    val diseaseEvents: Dataset[FlatEvent] = flatEvents.filter(_.category == "disease")
    val dcirFlat: DataFrame = sqlContext.read.parquet(FilteringConfig.inputPaths.dcir)

    val patients: Dataset[Patient] = flatEvents.map(
      e => Patient(e.patientID, e.gender, e.birthDate, e.deathDate)
    ).distinct
    val tracklossEvents: Dataset[Event] = TrackLossTransformer.transform(
      Sources(dcir=Some(dcirFlat))
    )
    val tracklossFlatEvents = tracklossEvents
      .as("left")
      .joinWith(patients.as("right"), col("left.patientID") === col("right.patientID"))
      .map((FlatEvent.merge _).tupled)
      .cache()

    val allEvents = flatEvents.union(tracklossFlatEvents)

    val exposures: Dataset[FlatEvent] = MLPPExposuresTransformer.transform(allEvents)

    val mlppParams = MLPPWriter.Params(
      bucketSize = MLPPConfig.bucketSize,
      lagCount = MLPPConfig.lagCount,
      minTimestamp = MLPPConfig.minTimestamp,
      maxTimestamp = MLPPConfig.maxTimestamp
    )
    val mlppWriter = MLPPWriter(mlppParams)
    val result = MLPPWriter(mlppParams).write(diseaseEvents.union(exposures), outputPath)

    Some(result)
  }
}
package fr.polytechnique.cmap.cnam.filtering.mlpp

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.hive.HiveContext
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.filtering.{FilteringConfig, FilteringMain, FlatEvent}

object MLPPMain extends Main {

  override def appName: String = "MLPPFeaturing"

  def run(sqlContext: HiveContext, argsMap: Map[String, String] = Map()): Option[Dataset[MLPPFeature]] = {

    // "get" returns an Option, then we can use foreach to gently ignore when the key was not found.
    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))

    val outputPath: String = FilteringConfig.outputPaths.mlppFeatures
    val flatEvents: Dataset[FlatEvent] = FilteringMain.run(sqlContext).get
      .filter(e => e.category == "molecule" || e.category == "disease").cache()

    val diseaseEvents: Dataset[FlatEvent] = flatEvents.filter(_.category == "disease")
    val exposures: Dataset[FlatEvent] = MLPPExposuresTransformer.transform(flatEvents)

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
package fr.polytechnique.cmap.cnam.etl.loaders.mlpp

import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.util.Path
import fr.polytechnique.cmap.cnam.util.RichDataFrame._
import fr.polytechnique.cmap.cnam.util.functions._

class MLPPLoader(params: MLPPLoader.Params = MLPPLoader.Params()) {

  private val bucketCount = (daysBetween(params.maxTimestamp, params.minTimestamp) / params.bucketSize).toInt
  
  def load(
    outcomes: Dataset[Event[Outcome]],
    exposures: Dataset[Event[Exposure]],
    patients: Dataset[Patient]): Dataset[MLPPFeature] = {
    

    val mlppDataFrame = new MLPPDataFrame(params)

    val richOutcomes: DataFrame = mlppDataFrame
      .makeMLPPDataFrame(outcomes.join(patients, "patientID"))
      .where(col("startBucket") < col("endBucket"))

    val richExposures: DataFrame = mlppDataFrame
      .makeMLPPDataFrame(exposures.join(patients, "patientID"))
      .where(col("startBucket") < col("endBucket"))

    val commonPatients = richOutcomes.select("patientID").distinct().join(
      richExposures.select("patientID").distinct(), "patientID"
    ).withIndices(Seq("patientID"))

    //write outcome data
    new MLPPOutcomes(params).writeOutcomes(richOutcomes.join(commonPatients, "patientID"))

    //write exposure data
    val mlppExposure = new MLPPExposures(params)
    val exposureEvents: Dataset[LaggedExposure] = mlppExposure
      .makeExposures(richExposures.join(commonPatients, "patientID"))
      .persist()
    val features: Dataset[MLPPFeature] = mlppExposure.writeExposures(exposureEvents)

    //write meta data and censoring data
    val discreteExposure = new DiscreteExposure(params)
    discreteExposure.writeMetadata(exposureEvents)
    discreteExposure.writeCensoring(exposureEvents)

    exposureEvents.unpersist()

    // Return the features for convenience
    features
  }
}


object MLPPLoader {
  
  case class Params(
    outputRootPath: Path = Path("/"), //it need to replace this default value when you use this
    bucketSize: Int = 1,
    lagCount: Int = 1,
    minTimestamp: Timestamp = makeTS(2006, 1, 1),
    maxTimestamp: Timestamp = makeTS(2009, 12, 31, 23, 59, 59),
    keepFirstOnly: Boolean = true,
    includeCensoredBucket: Boolean = false,
    featuresAsList: Boolean = false
  )
  
  def apply(params: Params = Params()) = new MLPPLoader(params)
  
}

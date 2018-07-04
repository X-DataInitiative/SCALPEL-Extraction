package fr.polytechnique.cmap.cnam.etl.loaders.mlpp

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.loaders.mlpp.config.MLPPConfig
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.util.RichDataFrame._

class MLPPLoader(params: MLPPConfig) {

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
  def apply(params: MLPPConfig) = new MLPPLoader(params)
}

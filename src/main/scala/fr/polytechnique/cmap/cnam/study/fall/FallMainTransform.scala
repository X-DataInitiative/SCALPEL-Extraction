// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall

import scala.collection.mutable
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.filters.PatientFilters
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.transformers.exposures.ExposuresTransformer
import fr.polytechnique.cmap.cnam.etl.transformers.interaction.NLevelInteractionTransformer
import fr.polytechnique.cmap.cnam.study.fall.codes._
import fr.polytechnique.cmap.cnam.study.fall.config.FallConfig
import fr.polytechnique.cmap.cnam.study.fall.follow_up.FallStudyFollowUps
import fr.polytechnique.cmap.cnam.study.fall.fractures.FracturesTransformer
import fr.polytechnique.cmap.cnam.util.Path
import fr.polytechnique.cmap.cnam.util.datetime.implicits._
import fr.polytechnique.cmap.cnam.util.reporting._

object FallMainTransform extends Main with FractureCodes {

  override def appName: String = "fall study transform"

  override def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {
    val format = new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
    val startTimestamp = new java.util.Date()
    val fallConfig = FallConfig.load(argsMap("conf"), argsMap("env"))
    val operationsMetadata = OperationMetadata.deserialize(argsMap("meta_bin"))
    transformExposures(operationsMetadata, fallConfig)
    transformOutcomes(operationsMetadata, fallConfig)

    // Write Metadata
    val metadata = MainMetadata(
      this.getClass.getName,
      startTimestamp,
      new java.util.Date(),
      operationsMetadata.values.toList
    )
    val metadataJson: String = metadata.toJsonString()

    OperationReporter.writeMetaData(metadataJson, "metadata_fall.json", argsMap("env"))
    None
  }

  def transformExposures(meta: mutable.Map[String, OperationMetadata], fallConfig: FallConfig):
  mutable.Map[String, OperationMetadata] = {

    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._
    val patients: Dataset[Patient]
    = spark.read.parquet(meta.get("extract_patients").get.outputPath)
      .as[Patient].cache()
    val drugPurchases: Dataset[Event[Drug]]
    = spark.read.parquet(meta.get("drug_purchases").get.outputPath)
      .as[Event[Drug]].cache()
    val controlDrugPurchases = spark.read.parquet(meta.get("control_drugs_purchases").get.outputPath)
      .as[Event[Drug]].cache()

    if (fallConfig.runParameters.startGapPatients) {
      import PatientFilters._
      val filteredPatients: Dataset[Patient] = patients
        .filterNoStartGap(drugPurchases, fallConfig.base.studyStart, fallConfig.patients.startGapInMonths)
        .cache()
      val filteredPatientsReport = OperationReporter.reportAsDataSet(
        "filter_patients",
        List("drug_purchases", "extract_patients"),
        OperationTypes.Patients,
        filteredPatients,
        Path(fallConfig.output.outputSavePath),
        fallConfig.output.saveMode
      )
      meta += {
        filteredPatientsReport.name -> filteredPatientsReport
      }
    }

    if (fallConfig.runParameters.exposures) {
      val exposures = {
        val definition = fallConfig.exposures
        val patientsWithFollowUp = FallStudyFollowUps.transform(
          patients,
          fallConfig.base.studyStart,
          fallConfig.base.studyEnd,
          fallConfig.patients.followupStartDelay
        )
        import patientsWithFollowUp.sparkSession.sqlContext.implicits._
        val followUps = patientsWithFollowUp.map(_._2).cache()
        val followUpReport = OperationReporter.reportAsDataSet(
          "follow_up",
          List("extract_patients"),
          OperationTypes.AnyEvents,
          followUps,
          Path(fallConfig.output.outputSavePath),
          fallConfig.output.saveMode
        )
        meta += {
          followUpReport.name -> followUpReport
        }
        val controlDrugExposures = new ExposuresTransformer(definition)
          .transform(patientsWithFollowUp, controlDrugPurchases)
        meta += {
          "control_drugs_exposures" ->
            OperationReporter
              .report(
                "control_drugs_exposures",
                List("control_drugs_purchases", "follow_up"),
                OperationTypes.Exposures,
                controlDrugExposures.toDF,
                Path(fallConfig.output.outputSavePath),
                fallConfig.output.saveMode
              )
        }
        new ExposuresTransformer(definition).transform(patientsWithFollowUp, drugPurchases).cache()
      }
      val exposuresReport = OperationReporter.reportAsDataSet(
        "exposures",
        List("drug_purchases"),
        OperationTypes.Exposures,
        exposures,
        Path(fallConfig.output.outputSavePath),
        fallConfig.output.saveMode
      )
      meta += {
        exposuresReport.name -> exposuresReport
      }

      val interactions = NLevelInteractionTransformer(fallConfig.interactions).transform(exposures).cache()
      val interactionReport = OperationReporter.reportAsDataSet(
        "interactions",
        List("exposures"),
        OperationTypes.Exposures,
        interactions.toDF,
        Path(fallConfig.output.outputSavePath),
        fallConfig.output.saveMode
      )

      meta += {
        interactionReport.name -> interactionReport
      }

    }
    meta
  }

  def transformOutcomes(meta: mutable.Map[String, OperationMetadata], fallConfig: FallConfig):
  mutable.Map[String, OperationMetadata] = {

    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    val diagnoses = spark.read.parquet(meta("diagnoses").outputPath)
      .as[Event[Diagnosis]]

    val acts = spark.read.parquet(meta("acts").outputPath)
      .as[Event[MedicalAct]]

    val liberalActs = spark.read.parquet(meta("liberal_acts").outputPath)
      .as[Event[MedicalAct]]

    if (fallConfig.runParameters.outcomes) {
      logger.info("Fractures")
      val fractures: Dataset[Event[Outcome]] = new FracturesTransformer(fallConfig)
        .transform(liberalActs, acts, diagnoses)
      val fractures_report = OperationReporter.reportAsDataSet(
        "fractures",
        List("acts"),
        OperationTypes.Outcomes,
        fractures,
        Path(fallConfig.output.outputSavePath),
        fallConfig.output.saveMode
      )
      meta += {
        fractures_report.name -> fractures_report
      }
    }
    meta
  }
}


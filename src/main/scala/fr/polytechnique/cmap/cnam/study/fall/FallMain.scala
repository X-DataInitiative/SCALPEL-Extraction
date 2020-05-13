// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall

import scala.collection.mutable
import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event, FollowUp, Outcome}
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.events.hospitalstays.McoHospitalStaysExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.patients.{AllPatientExtractor, PatientsConfig}
import fr.polytechnique.cmap.cnam.etl.filters.PatientFilters
import fr.polytechnique.cmap.cnam.etl.implicits
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.etl.transformers.drugprescription.DrugPrescriptionTransformer
import fr.polytechnique.cmap.cnam.etl.transformers.exposures.ExposureTransformer
import fr.polytechnique.cmap.cnam.etl.transformers.interaction.NLevelInteractionTransformer
import fr.polytechnique.cmap.cnam.etl.transformers.patients.PatientFilters
import fr.polytechnique.cmap.cnam.study.fall.codes._
import fr.polytechnique.cmap.cnam.study.fall.config.FallConfig
import fr.polytechnique.cmap.cnam.study.fall.extractors._
import fr.polytechnique.cmap.cnam.study.fall.follow_up.FallStudyFollowUps
import fr.polytechnique.cmap.cnam.study.fall.fractures.FracturesTransformer
import fr.polytechnique.cmap.cnam.study.fall.liberalActs.LiberalActsTransformer
import fr.polytechnique.cmap.cnam.study.fall.statistics.DiagnosisCounter
import fr.polytechnique.cmap.cnam.util.Path
import fr.polytechnique.cmap.cnam.util.datetime.implicits._
import fr.polytechnique.cmap.cnam.util.reporting.{MainMetadata, OperationMetadata, OperationReporter, OperationTypes}

object FallMain extends Main with FractureCodes {

  override def appName: String = "fall study"

  override def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {

    val format = new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
    val startTimestamp = new java.util.Date()
    val fallConfig = FallConfig.load(argsMap("conf"), argsMap("env"))

    import implicits.SourceReader
    val sources = Sources.sanitize(sqlContext.readSources(fallConfig.input,fallConfig.fileFormat))
    val dcir = sources.dcir.get.repartition(4000).persist()
    val mco = sources.mco.get.repartition(4000).persist()

    val operationsMetadata = computeOutcomes(sources, fallConfig)

    dcir.unpersist()
    mco.unpersist()

    // Write Metadata
    val metadata = MainMetadata(this.getClass.getName, startTimestamp, new java.util.Date(), operationsMetadata.toList)
    val metadataJson: String = metadata.toJsonString()

    OperationReporter
      .writeMetaData(metadataJson, "metadata_fall_" + format.format(startTimestamp) + ".json", argsMap("env"))

    None
  }

  def computeHospitalStays(sources: Sources, fallConfig: FallConfig): mutable.Buffer[OperationMetadata] = {
    val operationsMetadata = mutable.Buffer[OperationMetadata]()
    if (fallConfig.runParameters.hospitalStays) {
      val hospitalStays = McoHospitalStaysExtractor.extract(sources).cache()

      operationsMetadata += {
        OperationReporter
          .report(
            "extract_hospital_stays",
            List("MCO"),
            OperationTypes.HospitalStays,
            hospitalStays.toDF,
            Path(fallConfig.output.outputSavePath),
            fallConfig.output.saveMode
          )
      }
    }
    operationsMetadata
  }

  def computeExposures(sources: Sources, fallConfig: FallConfig): mutable.Buffer[OperationMetadata] = {

    val operationsMetadata = mutable.Buffer[OperationMetadata]()

    val optionDrugPurchases = if (fallConfig.runParameters.drugPurchases) {
      val drugPurchases = new DrugsExtractor(fallConfig.drugs).extract(sources).cache()
      operationsMetadata += {
        OperationReporter
          .report(
            "drug_purchases",
            List("DCIR"),
            OperationTypes.Dispensations,
            drugPurchases.toDF,
            Path(fallConfig.output.outputSavePath),
            fallConfig.output.saveMode
          )
      }
      Some(drugPurchases)
    } else {
      None
    }

    val optionAllPatients = if (fallConfig.runParameters.patients) {
      val allpatients = AllPatientExtractor.extract(sources).cache()
      operationsMetadata += {
        OperationReporter
          .report(
            "extract_raw_patients",
            List("DCIR", "MCO", "IR_BEN_R", "MCO_CE"),
            OperationTypes.Patients,
            allpatients.toDF,
            Path(fallConfig.output.outputSavePath),
            fallConfig.output.saveMode
          )
      }
      Some(allpatients)
    } else {
      None
    }

    val filteredpatientsconfig = new PatientFilters(PatientsConfig(fallConfig.base.studyStart)).filterPatients(optionAllPatients.get).cache()
    operationsMetadata += {
      OperationReporter
        .report(
          "extract_filtered_patients",
          List("DCIR", "MCO", "IR_BEN_R", "MCO_CE"),
          OperationTypes.Patients,
          filteredpatientsconfig.toDF,
          Path(fallConfig.output.outputSavePath),
          fallConfig.output.saveMode
        )
    }

    if (fallConfig.runParameters.startGapPatients) {
      import PatientFilters._
      val filteredPatients: Dataset[Patient] = filteredpatientsconfig
        .filterNoStartGap(optionDrugPurchases.get, fallConfig.base.studyStart, fallConfig.patients.startGapInMonths)
      operationsMetadata += {
        OperationReporter
          .report(
            "filter_patients",
            List("drug_purchases", "extract_patients"),
            OperationTypes.Patients,
            filteredPatients.toDF,
            Path(fallConfig.output.outputSavePath),
            fallConfig.output.saveMode
          )
      }
    }

    if (fallConfig.runParameters.exposures) {
      val exposures = {
        val definition = fallConfig.exposures
        val patientsWithFollowUp: Dataset[(Patient, Event[FollowUp])] = FallStudyFollowUps
          .transform(
            filteredpatientsconfig,
            fallConfig.base.studyStart,
            fallConfig.base.studyEnd,
            fallConfig.patients.followupStartDelay
          )
        import patientsWithFollowUp.sparkSession.sqlContext.implicits._
        val followUps = patientsWithFollowUp.map(e => e._2)
        operationsMetadata += {
          OperationReporter
            .report(
              "follow_up",
              List("extract_patients"),
              OperationTypes.AnyEvents,
              followUps.toDF,
              Path(fallConfig.output.outputSavePath),
              fallConfig.output.saveMode
            )
        }
        val controlDrugPurchases = ControlDrugs.extract(sources).cache()
        operationsMetadata += {
          OperationReporter
            .report(
              "control_drugs_purchases",
              List("DCIR"),
              OperationTypes.Dispensations,
              controlDrugPurchases.toDF,
              Path(fallConfig.output.outputSavePath),
              fallConfig.output.saveMode
            )
        }

        val controlDrugExposures = new ExposureTransformer(definition)
          .transform(patientsWithFollowUp.map(_._2))(controlDrugPurchases)
        operationsMetadata += {
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

        val prescriptions = new DrugPrescriptionTransformer().transform(optionDrugPurchases.get).cache()

        operationsMetadata += {
          OperationReporter
            .report(
              "prescriptions",
              List("drug_purchases"),
              OperationTypes.Dispensations,
              prescriptions.toDF,
              Path(fallConfig.output.outputSavePath),
              fallConfig.output.saveMode
            )
        }

        val prescriptionsExposures = new ExposureTransformer(definition)
          .transform(patientsWithFollowUp.map(_._2).distinct())(prescriptions.as[Event[Drug]]).cache()
        operationsMetadata += {
          OperationReporter
            .report(
              "prescriptions_exposures",
              List("prescriptions", "follow_up"),
              OperationTypes.Exposures,
              prescriptionsExposures.toDF,
              Path(fallConfig.output.outputSavePath),
              fallConfig.output.saveMode
            )
        }

        new ExposureTransformer(definition)
          .transform(patientsWithFollowUp.map(_._2).distinct())(optionDrugPurchases.get)
      }
      operationsMetadata += {
        OperationReporter
          .report(
            "exposures",
            List("drug_purchases"),
            OperationTypes.Exposures,
            exposures.toDF,
            Path(fallConfig.output.outputSavePath),
            fallConfig.output.saveMode
          )
      }

      val interactions = NLevelInteractionTransformer(fallConfig.interactions).transform(exposures).cache()
      operationsMetadata += {
        OperationReporter
          .report(
            "interactions",
            List("exposures"),
            OperationTypes.Exposures,
            interactions.toDF,
            Path(fallConfig.output.outputSavePath),
            fallConfig.output.saveMode
          )
      }
    }
    operationsMetadata
  }

  def computeOutcomes(sources: Sources, fallConfig: FallConfig): mutable.Buffer[OperationMetadata] = {

    val operationsMetadata = mutable.Buffer[OperationMetadata]()

    val optionDiagnoses = if (fallConfig.runParameters.diagnoses) {
      val diagnoses = new DiagnosisExtractor(fallConfig.diagnoses).extract(sources).persist()
      val diagnosesPopulation = DiagnosisCounter.process(diagnoses)
      operationsMetadata += {
        OperationReporter.reportDataAndPopulationAsDataSet(
          "diagnoses",
          List("MCO", "IR_IMB_R"),
          OperationTypes.Diagnosis,
          diagnoses,
          diagnosesPopulation,
          Path(fallConfig.output.outputSavePath),
          fallConfig.output.saveMode
        )
      }
      Some(diagnoses)
    } else {
      None
    }

    val (optionActs, optionLiberalActs, optionSurgeries) = if (fallConfig.runParameters.acts) {
      val (acts, surgeries) = new ActsExtractor(fallConfig.medicalActs).extract(sources)
      acts.cache()
      surgeries.cache()
      operationsMetadata += {
        OperationReporter
          .report(
            "acts",
            List("DCIR", "MCO", "MCO_CE"),
            OperationTypes.MedicalActs,
            acts.toDF,
            Path(fallConfig.output.outputSavePath),
            fallConfig.output.saveMode
          )
      }
      operationsMetadata += {
        OperationReporter
          .report(
            "fracture_surgeries",
            List("MCO"),
            OperationTypes.MedicalActs,
            surgeries.toDF,
            Path(fallConfig.output.outputSavePath),
            fallConfig.output.saveMode
          )
      }
      val liberalActs = LiberalActsTransformer.transform(acts).persist()
      operationsMetadata += {
        OperationReporter
          .report(
            "liberal_acts",
            List("acts"),
            OperationTypes.MedicalActs,
            liberalActs.toDF,
            Path(fallConfig.output.outputSavePath),
            fallConfig.output.saveMode
          )
      }
      (Some(acts), Some(liberalActs), Some(surgeries))
    } else {
      (None, None, None)
    }
    val optionHospitalDeaths = if (fallConfig.runParameters.hospitalDeaths) {
      val hospitalDeaths = new FallHospitalStayExtractor(SimpleExtractorCodes(List(Death.value))).extract(sources)
      operationsMetadata += {
        OperationReporter
          .report(
            "hospital_deaths",
            List("MCO"),
            OperationTypes.HospitalStays,
            hospitalDeaths.toDF,
            Path(fallConfig.output.outputSavePath),
            fallConfig.output.saveMode
          )
      }
      Some(hospitalDeaths)
    } else {
      None
    }

    if (fallConfig.runParameters.outcomes) {
      val fractures: Dataset[Event[Outcome]] = new FracturesTransformer(fallConfig)
              .transform(optionLiberalActs.get, optionActs.get, optionDiagnoses.get, optionSurgeries.get, optionHospitalDeaths.get)
      operationsMetadata += {
        OperationReporter
          .report(
            "fractures",
            List("acts", "diagnoses"),
            OperationTypes.Outcomes,
            fractures.toDF,
            Path(fallConfig.output.outputSavePath),
            fallConfig.output.saveMode
          )
      }
    }
    operationsMetadata
  }

  def computeControls(sources: Sources, fallConfig: FallConfig): mutable.Buffer[OperationMetadata] = {
    val operationsMetadata = mutable.Buffer[OperationMetadata]()

    val opioids = OpioidsExtractor.extract(sources).cache()
    operationsMetadata += {
      OperationReporter
        .report(
          "Opioids",
          List("DCIR"),
          OperationTypes.Dispensations,
          opioids.toDF,
          Path(fallConfig.output.outputSavePath),
          fallConfig.output.saveMode
        )
    }

    val ipp = IPPExtractor.extract(sources).cache()
    operationsMetadata += {
      OperationReporter
        .report(
          "IPP",
          List("DCIR"),
          OperationTypes.Dispensations,
          ipp.toDF,
          Path(fallConfig.output.outputSavePath),
          fallConfig.output.saveMode
        )
    }

    val cardiac = CardiacExtractor.extract(sources).cache()
    operationsMetadata += {
      OperationReporter
        .report(
          "Cardiac",
          List("DCIR"),
          OperationTypes.Dispensations,
          cardiac.toDF,
          Path(fallConfig.output.outputSavePath),
          fallConfig.output.saveMode
        )
    }

    val epileptics = EpilepticsExtractor.extract(sources).cache()
    operationsMetadata += {
      OperationReporter
        .report(
          "epileptics",
          List("MCO", "IMB"),
          OperationTypes.Diagnosis,
          epileptics.toDF,
          Path(fallConfig.output.outputSavePath),
          fallConfig.output.saveMode
        )
    }

    val hta = HTAExtractor.extract(sources).cache()
    operationsMetadata += {
      OperationReporter
        .report(
          "HTA",
          List("DCIR"),
          OperationTypes.Dispensations,
          hta.toDF,
          Path(fallConfig.output.outputSavePath),
          fallConfig.output.saveMode
        )
    }
    operationsMetadata
  }
}

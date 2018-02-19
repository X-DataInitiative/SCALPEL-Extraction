package fr.polytechnique.cmap.cnam.study.fall

import java.io.PrintWriter
import java.sql.Timestamp

import scala.collection.mutable
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.events.DcirAct
import fr.polytechnique.cmap.cnam.etl.extractors.acts.{MedicalActs, MedicalActsConfig}
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses.{Diagnoses, DiagnosesConfig}
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.{DrugClassificationLevel, DrugsExtractor}
import fr.polytechnique.cmap.cnam.etl.extractors.patients.{Patients, PatientsConfig}
import fr.polytechnique.cmap.cnam.etl.filters.PatientFilters
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.etl.transformers.exposures.ExposuresTransformer
import fr.polytechnique.cmap.cnam.study.fall.codes._
import fr.polytechnique.cmap.cnam.study.fall.exposures.FallStudyExposures
import fr.polytechnique.cmap.cnam.study.fall.follow_up.FallStudyFollowUps
import fr.polytechnique.cmap.cnam.util.Path
import fr.polytechnique.cmap.cnam.util.functions._
import fr.polytechnique.cmap.cnam.util.reporting.{MainMetadata, OperationMetadata, OperationReporter, OperationTypes}
import org.apache.spark.sql.{Dataset, SQLContext}

object FallMain extends Main with FractureCodes {

  trait Env {
    val FeaturingPath: Path
    val McoPath: Path
    val McoCePath: Path
    val DcirPath: Path
    val IrImbPath: Path
    val IrPhaPath: String
    val StudyStart: Timestamp
    val StudyEnd: Timestamp
  }

  object CmapEnv extends Env {
    override val FeaturingPath = Path("/shared/Observapur/featuring/")
    override val McoPath = Path("/shared/Observapur/staging/Flattening/flat_table/MCO")
    override val McoCePath = Path("/shared/Observapur/staging/Flattening/flat_table/MCO_ACE")
    override val DcirPath = Path("/shared/Observapur/staging/Flattening/flat_table/DCIR")
    override val IrImbPath = Path("/shared/Observapur/staging/Flattening/single_table/IR_IMB_R")
    override val IrBenPath = Path("/shared/Observapur/staging/Flattening/single_table/IR_BEN_R")
    override val StudyStart: Timestamp = makeTS(2010,1,1)
    override val StudyEnd: Timestamp = makeTS(2011,1,1)
    val IrPhaPath = "/shared/Observapur/staging/Flattening/single_table/IR_PHA_R"
  }

  object CmapTestEnv extends Env {
    override val FeaturingPath = Path("/shared/Observapur/testing/featuring/")
    override val McoPath = Path("/shared/Observapur/testing/MCO")
    override val McoCePath = Path("/shared/Observapur/testing/MCO_ACE")
    override val DcirPath = Path("/shared/Observapur/testing/DCIR")
    override val IrImbPath = CmapEnv.IrImbPath
    override val IrBenPath = CmapEnv.IrBenPath
    override val StudyStart: Timestamp = makeTS(2010,1,1)
    override val StudyEnd: Timestamp = makeTS(2011,1,1)
  }

  object FallEnv extends Env {
    override val FeaturingPath = Path("/shared/fall/staging/featuring/")
    override val McoPath = Path("/shared/fall/staging/flattening/flat_table/MCO")
    override val McoCePath = Path("/shared/fall/staging/flattening/flat_table/MCO_CE")
    override val DcirPath = Path("/shared/fall/staging/flattening/flat_table/DCIR")
    override val IrImbPath = Path("/shared/fall/staging/flattening/single_table/IR_IMB_R")
    override val IrBenPath = Path("/shared/fall/staging/flattening/single_table/IR_BEN_R")
    override val StudyStart: Timestamp = makeTS(2015,1,1)
    override val StudyEnd: Timestamp = makeTS(2016,1,1)
    val IrPhaPath = "/shared/fall/staging/flattening/single_table/IR_PHA_R"
  }

  object TestEnv extends Env {
    override val FeaturingPath = Path("target/test/output/")
    override val McoPath = Path("src/test/resources/test-input/MCO.parquet")
    override val McoCePath: Path = null
    override val DcirPath = Path("src/test/resources/test-input/DCIR.parquet")
    override val IrImbPath = Path("src/test/resources/test-input/IR_IMB_R.parquet")
    override val IrBenPath = Path("src/test/resources/test-input/IR_BEN_R.parquet")
    override val StudyStart: Timestamp = makeTS(2006,1,1)
    override val StudyEnd: Timestamp = makeTS(2010,1,1)
    val IrPhaPath = "src/test/resources/test-input/IR_PHA_R.parquet"
  }

  def getSource(sqlContext: SQLContext, env: Env): Sources = {
    Sources.read(
      sqlContext,
      irImbPath = Option(env.IrImbPath).map(_.toString),
      irBenPath = Option(env.IrBenPath).map(_.toString),
      dcirPath = Option(env.DcirPath).map(_.toString),
      pmsiMcoPath = Option(env.McoPath).map(_.toString),
      pmsiMcoCEPath = Option(env.McoCePath).map(_.toString)
      irPhaPath = env.IrPhaPath
    )
  }

  def getEnv(argsMap: Map[String, String]): Env = {
    argsMap.getOrElse("env", "test") match {
      case "cmap" => CmapEnv
      case "cmap-test" => CmapTestEnv
      case "fall" => FallEnv
      case "test" => TestEnv
    }
  }

  override def appName: String = "fall study"

  override def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {

    val startTimestamp = new java.util.Date()

    val env = getEnv(argsMap)

    val source = getSource(sqlContext, env)
    val dcir = source.dcir.get.persist()
    val mco = source.pmsiMco.get.persist()
    val irPhaR = source.irPha.get.cache()

    val fracturesCodes = BodySite.extractCIM10CodesFromSites(List(BodySites))
    val fracturesPath = Path(env.FeaturingPath, "fractures")

    val operationsMetadata = mutable.Buffer[OperationMetadata]()

    // Extract Patients
    val patients = new Patients(PatientsConfig(env.StudyStart)).extract(source).cache()
    operationsMetadata += {
      OperationReporter.report("extract_patients", List("DCIR", "MCO", "IR_BEN_R"), OperationTypes.Patients, patients.toDF, env.FeaturingPath)
    }

    // Drug Purchases
    val drugPurchases = {
      new TherapeuticDrugs(dcir, List(Antidepresseurs, Hypnotiques, Neuroleptiques, Antihypertenseurs))
        .extract.persist()
    }
    operationsMetadata += {
      OperationReporter.report("drug_purchases", List("DCIR"), OperationTypes.Dispensations, drugPurchases.toDF, env.FeaturingPath)
    }

    // Medical Acts
    val codesCCAM = (NonHospitalizedFracturesCcam ++ CCAMExceptions).toList
    val acts = new MedicalActs(
       MedicalActsConfig(
         dcirCodes = codesCCAM,
         mcoCECodes = codesCCAM
       )
    ).extract(source).persist()
    operationsMetadata += {
      OperationReporter.report("acts", List("DCIR", "MCO", "MCO_CE"), OperationTypes.MedicalActs, acts.toDF, env.FeaturingPath)
    }
    dcir.unpersist()

    // Diagnoses
    val diagnoses = new Diagnoses(DiagnosesConfig(dpCodes = fracturesCodes, daCodes = fracturesCodes)).extract(source).persist()
    operationsMetadata += {
      OperationReporter.report("diagnoses", List("MCO", "IR_IMB_R"), OperationTypes.Diagnosis, diagnoses.toDF, env.FeaturingPath)
    }
    mco.unpersist()

    // Filter Patients
    import PatientFilters._
    val filteredPatients: Dataset[Patient] = patients.filterNoStartGap(drugPurchases, env.StudyStart, 2)
    operationsMetadata += {
      OperationReporter.report("filter_patients", List("drug_purchases", "extract_patients"), OperationTypes.Patients, filteredPatients.toDF, env.FeaturingPath)
    }
    patients.unpersist()

    // Exposures
    val exposures = {
      val definition = FallStudyExposures.fallMainExposuresDefinition(env.StudyStart)
      val patientsWithFollowUp = FallStudyFollowUps.transform(patients, env.StudyStart, env.StudyEnd, 2)
      new ExposuresTransformer(definition).transform(patientsWithFollowUp, drugPurchases)
    }
    operationsMetadata += {
      OperationReporter.report("exposures", List("drug_purchases"), OperationTypes.Exposures, exposures.toDF, env.FeaturingPath)
    }
    drugPurchases.unpersist()

    // Liberal Medical Acts
    val liberalActs = acts.filter(act =>
      act.groupID == DcirAct.groupID.Liberal && !CCAMExceptions.contains(act.value)).persist()
    operationsMetadata += {
      OperationReporter.report("liberal_acts", List("acts"), OperationTypes.MedicalActs, liberalActs.toDF, env.FeaturingPath)
    }

    // Liberal Fractures
    val liberalFractures = LiberalFractures.transform(liberalActs)
    operationsMetadata += {
      OperationReporter.report("liberal_fractures", List("liberal_acts"), OperationTypes.Outcomes, liberalFractures.toDF, fracturesPath)
    }
    liberalActs.unpersist()

    // Hospitalized Fractures
    val hospitalizedFractures = HospitalizedFractures.transform(diagnoses, acts, List(BodySites))
    operationsMetadata += {
      OperationReporter.report("hospitalized_fractures", List("diagnoses", "acts"), OperationTypes.Outcomes, hospitalizedFractures.toDF, env.FeaturingPath)
    }

    // Public Ambulatory Fractures
    val publicAmbulatoryFractures = PublicAmbulatoryFractures.transform(acts)
    operationsMetadata += {
      OperationReporter.report("public_ambulatory_fractures", List("acts"), OperationTypes.Outcomes, publicAmbulatoryFractures.toDF, fracturesPath)
    }

    // Private Ambulatory Fractures
    val privateAmbulatoryFractures =  PrivateAmbulatoryFractures.transform(acts)
    operationsMetadata += {
      OperationReporter.report("private_ambulatory_fractures", List("acts"), OperationTypes.Outcomes, privateAmbulatoryFractures.toDF, fracturesPath)
    }
    acts.unpersist()

    // Write Metadata
    val metadata = MainMetadata(this.getClass.getName, startTimestamp, new java.util.Date(), operationsMetadata.toList)
    val metadataJson: String = metadata.toJsonString()
    logger.info("Drug Purchases")
    val drugPurchases = DrugsExtractor
      .extract(DrugClassificationLevel.Therapeutic, source, List(Antidepresseurs, Hypnotiques, Neuroleptiques, Antihypertenseurs))
      .cache()

    logger.info("  count: " + drugPurchases.count)
    logger.info("  count distinct: " + drugPurchases.distinct.count)

    new PrintWriter("metadata.json") {
      write(metadataJson)
      close()
    }

    None
  }
}

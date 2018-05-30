package fr.polytechnique.cmap.cnam.study.prostatepostop

import java.io.PrintWriter
import java.sql.Timestamp

import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.extractors.acts.{MedicalActs, MedicalActsConfig}
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses.{Diagnoses, DiagnosesConfig}
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.{DrugsExtractor, PharmacologicalLevel}
import fr.polytechnique.cmap.cnam.etl.extractors.patients.{Patients, PatientsConfig}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.Path
import fr.polytechnique.cmap.cnam.util.functions.makeTS
import fr.polytechnique.cmap.cnam.util.reporting.{MainMetadata, OperationMetadata, OperationReporter, OperationTypes}
import org.apache.spark.sql.{Dataset, SQLContext}

import scala.collection.mutable

object ProstatePostOpMain extends Main {
  trait Env {
    val FeaturingPath: Path
    val McoPath: Path
    val McoCePath: Path
    val DcirPath: Path
    val IrImbPath: Path
    val IrBenPath: Path
    val IrPhaPath: Path
    val StudyStart: Timestamp
    val StudyEnd: Timestamp
  }

  object CmapEnv extends Env {
    override val FeaturingPath = Path("/shared/ProstatePostOp/featuring/")
    override val McoPath = Path("/shared/Observapur/staging/Flattening/flat_table/MCO")
    override val McoCePath = Path("/shared/Observapur/staging/Flattening/flat_table/MCO_ACE")
    override val DcirPath = Path("/shared/Observapur/staging/Flattening/flat_table/DCIR")
    override val IrImbPath = Path("/shared/Observapur/staging/Flattening/single_table/IR_IMB_R")
    override val IrBenPath = Path("/shared/Observapur/staging/Flattening/single_table/IR_BEN_R")
    override val StudyStart: Timestamp = makeTS(2010,1,1)
    override val StudyEnd: Timestamp = makeTS(2015,1,1)
    override val IrPhaPath = Path("/shared/Observapur/staging/Flattening/single_table/IR_PHA_R")
  }

  def readSources(sqlContext: SQLContext, env: Env): Sources = {
    Sources.read(
      sqlContext,
      irImbPath = Option(env.IrImbPath).map(_.toString),
      irBenPath = Option(env.IrBenPath).map(_.toString),
      dcirPath = Option(env.DcirPath).map(_.toString),
      mcoPath = Option(env.McoPath).map(_.toString),
      mcoCePath = Option(env.McoCePath).map(_.toString),
      irPhaPath = Option(env.IrPhaPath).map(_.toString)
    )
  }

  def getEnv(argsMap: Map[String, String]): Env = {
    argsMap.getOrElse("env", "test") match {
      case "cmap" => CmapEnv
    }
  }

  override def appName: String = "Prostate post operation study"

  override def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {

    val startTimestamp = new java.util.Date()

    val env = getEnv(argsMap)

    val sources = Sources.sanitize(readSources(sqlContext, env))

    val operationsMetadata = mutable.Buffer[OperationMetadata]()

    val selectedPatientsPath = "/shared/ProstatePostOp/20180528cohort-info.parquet"
    val selectedPatients = sqlContext.read.parquet(selectedPatientsPath)
    import sqlContext.implicits._

    // Extract Patients
    val patients = new Patients(PatientsConfig(env.StudyStart)).extract(sources)
    operationsMetadata += {
      OperationReporter.report("extract_patients", List("DCIR", "MCO", "IR_BEN_R"),
        OperationTypes.Patients, patients.toDF.join(selectedPatients,
          "patientID"), env.FeaturingPath)
    }

    // Extract Drug purchases
    // extract all drugs, at drug level
    logger.info("Drug Purchases")
    val drugPurchases = DrugsExtractor
      .extract(PharmacologicalLevel, sources, Nil) // ODO: add custom level which don't filter anything
      .cache()
    operationsMetadata += {
      OperationReporter.report("drug_purchases", List("DCIR"),
        OperationTypes.Dispensations, drugPurchases.toDF.join(selectedPatients,
          "patientID"), env.FeaturingPath)
    }



    // Medical Acts
    val acts = new MedicalActs(
      MedicalActsConfig(
        dcirCodes = Nil,
        mcoCECodes = Nil
      )
    ).extract(sources)
    operationsMetadata += {
      OperationReporter.report("acts", List("DCIR", "MCO", "MCO_CE"),
        OperationTypes.MedicalActs, acts.toDF.join(selectedPatients,
          "patientID"), env.FeaturingPath)
    }

    // Diagnoses
    val diagnoses = new Diagnoses(DiagnosesConfig(dpCodes = Nil, daCodes = Nil)).extract(sources).persist()
    operationsMetadata += {
      OperationReporter.report("diagnoses", List("MCO", "IR_IMB_R"),
        OperationTypes.Diagnosis, diagnoses.toDF.join(selectedPatients,
          "patientID"), env.FeaturingPath)
    }
    // Write Metadata
    val metadata = MainMetadata(this.getClass.getName, startTimestamp, new java.util.Date(), operationsMetadata.toList)
    val metadataJson: String = metadata.toJsonString()

    new PrintWriter("metadata.json") {
      write(metadataJson)
      close()
    }

    None
  }

}

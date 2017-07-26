package fr.polytechnique.cmap.cnam.study.fall

import java.sql.Timestamp
import org.apache.spark.sql.{Dataset, SQLContext, SaveMode}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.extractors.acts.{MedicalActs, MedicalActsConfig}
import fr.polytechnique.cmap.cnam.etl.extractors.classifications.GHMClassifications
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses.{Diagnoses, DiagnosesConfig}
import fr.polytechnique.cmap.cnam.etl.extractors.patients.{Patients, PatientsConfig}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.study.fall.codes.FractureCodes
import fr.polytechnique.cmap.cnam.util.functions.makeTS

object StudyMain extends Main with FractureCodes {

  trait Env {
    val OutRootPath: String = ""
    val McoPath: String
    val McoCePath: String
    val DcirPath: String
    val IrImbPath: String
    val IrBenPath: String
    val RefDate: Timestamp
  }

  object CmapEnv extends Env {
    val McoPath = "/shared/Observapur/staging/Flattening/flat_table/MCO"
    val McoCePath = "/shared/Observapur/staging/Flattening/flat_table/MCO_ACE"
    val DcirPath = "/shared/Observapur/staging/Flattening/flat_table/DCIR"
    val IrImbPath = "/shared/Observapur/staging/Flattening/single_table/IR_IMB_R"
    val IrBenPath = "/shared/Observapur/staging/Flattening/single_table/IR_BEN_R"
    val RefDate = makeTS(2010,1,1)
  }

  object FallEnv extends Env {
    val McoPath = "/shared/fall/staging/flattening/flat_table/MCO"
    val McoCePath = "/shared/fall/staging/flattening/flat_table/MCO_ACE"
    val DcirPath = "/shared/fall/staging/flattening/flat_table/DCIR"
    val IrImbPath = "/shared/fall/staging/flattening/single_table/IR_IMB_R"
    val IrBenPath = "/shared/fall/staging/flattening/single_table/IR_BEN_R"
    val RefDate = makeTS(2015,1,1)
  }

  object TestEnv extends Env {
    override val OutRootPath = "target/test/output/"
    val McoPath = "src/test/resources/test-input/MCO.parquet"
    val McoCePath = ""
    val DcirPath = "src/test/resources/test-input/DCIR.parquet"
    val IrImbPath = "src/test/resources/test-input/IR_IMB_R.parquet"
    val IrBenPath = "src/test/resources/test-input/IR_BEN_R.parquet"
    val RefDate = makeTS(2006,1,1)
  }

  def getSource(sqlContext: SQLContext, env: Env): Sources = {
    Sources.read(
      sqlContext,
      irImbPath = env.IrImbPath,
      irBenPath = env.IrBenPath,
      dcirPath = env.DcirPath,
      pmsiMcoPath = env.McoPath,
      pmsiMcoCEPath = env.McoCePath
    )
  }

  def getEnv(argsMap: Map[String, String]): Env = {
    argsMap.getOrElse("env", "test") match {
      case "fall" => FallEnv
      case "cmap" => CmapEnv
      case "test" => TestEnv
    }
  }

  override def appName: String = "fall study"

  override def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] ={

    val env = getEnv(argsMap)

    val source = getSource(sqlContext, env)

    val patients = new Patients(PatientsConfig(env.RefDate)).extract(source).cache()

    logger.info("Diagnoses")
    val diagnoses = new Diagnoses(DiagnosesConfig(dpCodes = HospitalizedFracturesCim10)).extract(source).cache()
    logger.info("  count: " + diagnoses.count)
    logger.info("  count distinct: " + diagnoses.distinct.count)

    logger.info("Classifications")
    val classifications = GHMClassifications.extract(source.pmsiMco.get, GenericGHMCodes).cache()
    logger.info("  count: " + classifications.count)
    logger.info("  count distinct: " + classifications.distinct.count)

    val acts = new MedicalActs(
      MedicalActsConfig(
        dcirCodes = GenericCCAMCodes,
        mcoCECodes = GenericCCAMCodes
      )).extract(source).cache()

    logger.info("Outcomes")
    val hospitalizedOutcomes = HospitalizedFall.transform(diagnoses, classifications).cache()
    val privateOutcomes = PrivateAmbulatoryFall.transform(acts)
    val publicOutcomes = PublicAmbulatoryFall.transform(acts)
    val outcomes = hospitalizedOutcomes.union(privateOutcomes).union(publicOutcomes)
    logger.info("  count: " + outcomes.count)
    logger.info("  count distinct: " + outcomes.distinct.count)

    logger.info("Patients with outcomes")
    logger.info("  count: " + outcomes.select("patientID").distinct.count)

    logger.info("Writing")
    logger.info("  Diagnoses...")
    diagnoses.write.mode(SaveMode.Overwrite).parquet(env.OutRootPath + "diagnoses")
    logger.info("  Classification...")
    classifications.write.mode(SaveMode.Overwrite).parquet(env.OutRootPath + "classification")
    logger.info("  Outcomes...")
    outcomes.write.mode(SaveMode.Overwrite).parquet(env.OutRootPath + "outcomes")
    logger.info("  Patients...")
    patients.write.mode(SaveMode.Overwrite).parquet(env.OutRootPath + "patients")

    Some(outcomes)
  }
}

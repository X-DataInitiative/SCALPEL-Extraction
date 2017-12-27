package fr.polytechnique.cmap.cnam.study.fall

import java.sql.Timestamp

import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.extractors.acts.{MedicalActs, MedicalActsConfig}
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses.{Diagnoses, DiagnosesConfig}
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.TherapeuticDrugs
import fr.polytechnique.cmap.cnam.etl.extractors.patients.{Patients, PatientsConfig}
import fr.polytechnique.cmap.cnam.etl.filters.{EventFilters, PatientFilters}
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.study.fall.codes._
import fr.polytechnique.cmap.cnam.util.functions._
import org.apache.spark.sql.{Dataset, SQLContext, SaveMode}

object FallMain extends Main with FractureCodes {

  trait Env {
    val FeaturingPath: String
    val McoPath: String
    val McoCePath: String
    val DcirPath: String
    val IrImbPath: String
    val IrBenPath: String
    val RefDate: Timestamp
  }

  object CmapEnv extends Env {
    override val FeaturingPath = "/shared/Observapur/featuring/"
    val McoPath = "/shared/Observapur/staging/Flattening/flat_table/MCO"
    val McoCePath = "/shared/Observapur/staging/Flattening/flat_table/MCO_ACE"
    val DcirPath = "/shared/Observapur/staging/Flattening/flat_table/DCIR"
    val IrImbPath = "/shared/Observapur/staging/Flattening/single_table/IR_IMB_R"
    val IrBenPath = "/shared/Observapur/staging/Flattening/single_table/IR_BEN_R"
    val RefDate = makeTS(2010,1,1)
  }

  object FallEnv extends Env {
    override val FeaturingPath = "/shared/fall/20171408/featuring/"
    val McoPath = "/shared/fall/staging/flattening/flat_table/MCO"
    val McoCePath = "/shared/fall/staging/flattening/flat_table/MCO_ACE"
    val DcirPath = "/shared/fall/staging/flattening/flat_table/DCIR"
    val IrImbPath = "/shared/fall/staging/flattening/single_table/IR_IMB_R"
    val IrBenPath = "/shared/fall/staging/flattening/single_table/IR_BEN_R"
    val RefDate = makeTS(2015,1,1)
  }

  object TestEnv extends Env {
    override val FeaturingPath = "target/test/output/"
    val McoPath = "src/test/resources/test-input/MCO.parquet"
    val McoCePath = null
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

  override def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {

//    import EventFilters._
    import PatientFilters._

    val env = getEnv(argsMap)
    val source = getSource(sqlContext, env)
    val dcir = source.dcir.get.cache()
    val mco = source.pmsiMco.get.cache()
    val patients = new Patients(PatientsConfig(env.RefDate)).extract(source).cache()

    logger.info("Drug Purchases")
    val drugPurchases = {
      new TherapeuticDrugs(dcir, List(Antidepresseurs, Hypnotiques, Neuroleptiques, Antihypertenseurs))
        .extract
        .cache()
    }
    logger.info("  count: " + drugPurchases.count)
    logger.info("  count distinct: " + drugPurchases.distinct.count)


    logger.info("Filtering Patients")
    logger.info("  count before: " + patients.count)
    // todo: the number of months should be a runtime parameter!
    val filteredPatients: Dataset[Patient] = patients.filterDelayedPatients(drugPurchases, env.RefDate, 2).cache()
    logger.info("  count after: " + filteredPatients.count)

    logger.info("Diagnoses")
    val fracturesCodes = Site.extractCodeSites(List(BodySites))
    val diagnoses = new Diagnoses(DiagnosesConfig(dpCodes = fracturesCodes, daCodes = fracturesCodes)).extract(source).repartition(215).cache()
    logger.info("  count: " + diagnoses.count)
    logger.info("  count distinct: " + diagnoses.distinct.count)

    logger.info("  Diagnoses...")
    diagnoses.write.mode(SaveMode.Overwrite).parquet(env.FeaturingPath + "diagnosesSites")

    logger.info("MedicalActs MCO")
    val hospitMedicalActs = new MedicalActs(
      MedicalActsConfig(
        mcoCCAMCodes = CCAMExceptions.toList
      )
    ).extract(source).cache()

    logger.info("  count: " + hospitMedicalActs.count)
    logger.info("  count distinct: " + hospitMedicalActs.distinct.count)

    logger.info("  hospitMedicalActs...")
    hospitMedicalActs.write.mode(SaveMode.Overwrite).parquet(env.FeaturingPath + "hospitMedicalActs")

    val acts = new MedicalActs(
      MedicalActsConfig(
        dcirCodes = NonHospitalizedFracturesCcam.toList,
        mcoCECodes = NonHospitalizedFracturesCcam.toList
      )
    ).extract(source).cache()

    logger.info("Outcomes")
    logger.info("hospitFractures")
    val hospitFractures = HospitalizedFractures.transform(diagnoses, hospitMedicalActs, List(BodySites)).cache()
    logger.info("  count: " + hospitFractures.count)
    logger.info("  count distinct: " + hospitFractures.distinct.count)

    logger.info("privateAmbualtoryFractures")
    val privateAmbualtoryFractures =  PrivateAmbulatoryFractures.transform(acts)
    logger.info("  count: " + privateAmbualtoryFractures.count)
    logger.info("  count distinct: " + privateAmbualtoryFractures.distinct.count)

    logger.info("publicAmbulatoryFractures")
    val publicAmbulatoryFractures = PublicAmbulatoryFractures.transform(acts)
    logger.info("  count: " + publicAmbulatoryFractures.count)
    logger.info("  count distinct: " + publicAmbulatoryFractures.distinct.count)


    logger.info("Writing")
    logger.info("  Drug Purchases...")
    drugPurchases.write.mode(SaveMode.Overwrite).parquet(env.FeaturingPath + "drug-purchases")

    logger.info("  Outcomes...")
    hospitFractures.write.mode(SaveMode.Overwrite).parquet(env.FeaturingPath + "fractures/hospitFractures")
    publicAmbulatoryFractures.write.mode(SaveMode.Overwrite).parquet(env.FeaturingPath + "fractures/publicAmbulatoryFractures")
    privateAmbualtoryFractures.write.mode(SaveMode.Overwrite).parquet(env.FeaturingPath + "fractures/privateAmbualtoryFractures")

    logger.info("  Patients...")
    patients.write.mode(SaveMode.Overwrite).parquet(env.FeaturingPath + "patients-total")
    filteredPatients.write.mode(SaveMode.Overwrite).parquet(env.FeaturingPath + "patients-filtered")

    Some(hospitFractures)
  }
}

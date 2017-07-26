package fr.polytechnique.cmap.cnam.study.fall

import java.sql.Timestamp

import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.extractors.classifications.GHMClassifications
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses.{Diagnoses, DiagnosesConfig}
import fr.polytechnique.cmap.cnam.etl.extractors.patients.{Patients, PatientsConfig}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.makeTS
import org.apache.spark.sql.{Dataset, SQLContext, SaveMode}

object StudyMain extends Main with FallStudyCodes{

  trait Env {
    val MCO: String
    val DCIR: String
    val IMB: String
    val BEN: String
    val RefDate: Timestamp
  }

  object CMAP extends Env {
    val MCO = "/shared/Observapur/staging/Flattening/flat_table/MCO"
    val DCIR = "/shared/Observapur/staging/Flattening/flat_table/DCIR"
    val IMB = "/shared/Observapur/staging/Flattening/single_table/IR_IMB_R"
    val BEN = "/shared/Observapur/staging/Flattening/single_table/IR_BEN_R"
    val RefDate = makeTS(2010,1,1)
  }

  object FALL extends Env {
    val MCO = "/shared/fall/staging/flattening/flat_table/MCO"
    val DCIR = "/shared/fall/staging/flattening/flat_table/DCIR"
    val IMB = "/shared/fall/staging/flattening/single_table/IR_IMB_R"
    val BEN = "/shared/fall/staging/flattening/single_table/IR_BEN_R"
    val RefDate = makeTS(2015,1,1)
  }

  object TEST extends Env {
    val MCO = "src/test/resources/test-input/MCO.parquet"
    val DCIR = "src/test/resources/test-input/DCIR.parquet"
    val IMB = "src/test/resources/test-input/IR_IMB_R.parquet"
    val BEN = "src/test/resources/test-input/IR_BEN_R.parquet"
    val RefDate = makeTS(2006,1,1)
  }

  def getSource(sqlContext: SQLContext, env: Env): Sources = {
    Sources(
      sqlContext = sqlContext,
      irImbPath = env.IMB,
      irBenPath = env.BEN,
      dcirPath = env.DCIR,
      pmsiMcoPath = env.MCO
    )
  }

  def getEnv(argsMap: Map[String, String]): Env = {
    val name = argsMap.getOrElse("env", "test")
    if (name == "fall") {
      FALL
    } else if (name == "cmap") {
      CMAP
    } else {
      TEST
    }
  }

  override def appName: String = "fall study"

  override def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] ={

    val env = getEnv(argsMap)

    val source = getSource(sqlContext, env)

    val patients = new Patients(
      PatientsConfig(
        env.RefDate
      )).extract(source)

    val diagnoses = new Diagnoses(
        DiagnosesConfig(
            dpCodes = GenericCIM10Codes
        )).extract(source).cache()

    val classifications = GHMClassifications.extract(source.pmsiMco.get, GenericGHMCodes).cache()

    val outcomes = HospitalizedFall.transform(diagnoses, classifications).cache()

    logger.info("Diagnoses")
    logger.info(diagnoses.count)
    logger.info(diagnoses.distinct.count)
    logger.info("classifications")
    logger.info(classifications.count)
    logger.info(classifications.distinct.count)
    logger.info("outcomes")
    logger.info(outcomes.count)
    logger.info(outcomes.distinct.count)
    logger.info("patients with outcomes")
    logger.info(outcomes.select("patientID").distinct.count)

    diagnoses.write.mode(SaveMode.Overwrite).parquet("diagnoses")
    classifications.write.mode(SaveMode.Overwrite).parquet("classification")
    outcomes.write.mode(SaveMode.Overwrite).parquet("outcomes")
    patients.write.mode(SaveMode.Overwrite).parquet("patients")

    Some(outcomes)
  }
}

// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.config

import java.io.File
import java.nio.file.Paths
import java.time.LocalDate
import com.typesafe.config.ConfigFactory
import me.danielpes.spark.datetime.implicits._
import org.scalatest.flatspec.AnyFlatSpec
import fr.polytechnique.cmap.cnam.etl.config.BaseConfig
import fr.polytechnique.cmap.cnam.etl.config.study.StudyConfig.{InputPaths, OutputPaths}
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.level.PharmacologicalLevel
import fr.polytechnique.cmap.cnam.etl.transformers.exposures.{LatestPurchaseBased, LimitedExposureAdder}

class FallConfigSuite extends AnyFlatSpec {

  val inputPaths = InputPaths(
    dcir = Some("src/test/resources/test-input/DCIR.parquet"),
    mco = Some("src/test/resources/test-input/MCO.parquet"),
    mcoCe = Some("src/test/resources/test-input/MCO_CE.parquet"),
    irBen = Some("src/test/resources/test-input/IR_BEN_R.parquet"),
    irImb = Some("src/test/resources/test-input/IR_IMB_R.parquet"),
    irPha = Some("src/test/resources/test-input/IR_PHA_R_With_molecules.parquet")
  )

  val outputPaths = OutputPaths(
    root = "target/test/output"
  )

  val base = new BaseConfig(
    ageReferenceDate = LocalDate.of(2015, 1, 1),
    studyStart = LocalDate.of(2014, 1, 1),
    studyEnd = LocalDate.of(2032, 1, 1)
  )

  "load" should "load default config file" in {
    //Given
    val expected = FallConfig(inputPaths, outputPaths, base)
    //When
    val result = FallConfig.load("", "test")
    //Then
    assert(result == expected)
  }

  "load" should "load the correct config file" in {
    //Given
    val defaultConf = FallConfig.load("", "test")
    val tempPath = "target/test.conf"
    val stringConfig =
      """
        | input {
        |   mco: "new/in/path"
        | }
        | output {
        |   root: "new/out/path"
        | }
        | exposures {
        |    exposure_period_adder: {
        |      exposure-adder-strategy = "n-limited-exposure-adder"
        |      start_delay = 10 days
        |      end_delay = 1 days
        |      end_threshold_gc = 900 days
        |      end_threshold_ngc = 300 days
        |      to_exposure_strategy  = "latest_purchase_based"
        |    }
        |  }
        |  interaction {
        |    level: 5
        |  }
        |  patients {
        |  start_gap_in_months: 2
        |  }
        |  drugs {
        |    level: "Pharmacological"
        |    families: ["Antihypertenseurs", "Antidepresseurs", "Neuroleptiques", "Hypnotiques"]
        |  }
        |  sites {
        |    sites: ["BodySites"]
        |  }
        |  run_parameters {
        |    outcome: ["Acts", "Diagnoses", "Outcomes"] // pipeline of calculation of outcome, possible values : Acts, Diagnoses, and Outcomes
        |    exposure: ["Patients", "DrugPurchases", "Exposures"] // pipeline of the calculation of exposure, possible values : Patients, StartGapPatients, DrugPurchases, Exposures
        |  }
        |  """.trim.stripMargin

    val expected = defaultConf.copy(
      input = defaultConf.input.copy(
        mco = Some("new/in/path")
      ), output = defaultConf.output.copy(
        root = "new/out/path"
      ), exposures = defaultConf.exposures.copy(
        LimitedExposureAdder(
          startDelay = 10.days,
          endDelay = 1.days,
          endThresholdNgc = 300.days,
          endThresholdGc = 900.days,
          toExposureStrategy = LatestPurchaseBased
        )
      ), drugs = defaultConf.drugs.copy(
        level = PharmacologicalLevel
      ), runParameters = defaultConf.runParameters.copy(exposure = List("Patients", "DrugPurchases", "Exposures"))
    )
    //When
    pureconfig.saveConfigAsPropertyFile(ConfigFactory.parseString(stringConfig), Paths.get(tempPath), true)
    val result = FallConfig.load(tempPath, "test")
    //Then
    try assert(result == expected)
    finally
      new File(tempPath).delete()
  }
}

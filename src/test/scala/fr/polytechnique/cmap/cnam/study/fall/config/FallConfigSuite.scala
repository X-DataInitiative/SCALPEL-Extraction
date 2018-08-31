package fr.polytechnique.cmap.cnam.study.fall.config

import java.nio.file.Paths
import com.typesafe.config.ConfigFactory
import me.danielpes.spark.datetime.implicits._
import org.scalatest.FlatSpec
import fr.polytechnique.cmap.cnam.etl.config.study.StudyConfig.{InputPaths, OutputPaths}
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.PharmacologicalLevel

class FallConfigSuite extends FlatSpec{

  val inputPaths = InputPaths(
    dcir = Some("src/test/resources/test-input/DCIR.parquet"),
    mco = Some("src/test/resources/test-input/MCO.parquet"),
    mcoCe = None,
    irBen = Some("src/test/resources/test-input/IR_BEN_R.parquet"),
    irImb = Some("src/test/resources/test-input/IR_IMB_R.parquet"),
    irPha = Some("src/test/resources/test-input/IR_PHA_R_With_molecules.parquet")
  )

  val outputPaths = OutputPaths(
    root = "target/test/output"
  )
  "load" should "load default config file" in {
    //Given
    val expected = FallConfig(inputPaths, outputPaths)
    //When
    val result = FallConfig.load("", "test")
    //Then
    assert(result == expected)
  }

  it should "load the correct config file" in {
    //Given
    val defaultConf = FallConfig.load("", "test")
    val tempPath = "target/test.conf"
    val stringConfig = """
                         | input {
                         |   mco: "new/in/path"
                         | }
                         | output {
                         |   root: "new/out/path"
                         | }
                         | exposures {
                         |    min_purchases: 2           // 1+ (Usually 1 or 2)
                         |    start_delay: 0 months      // 0+ (Usually between 0 and 3). Represents the delay in months between a dispensation and its exposure start date.
                         |    purchases_window: 0 months // 0+ (Usually 0 or 6) Represents the window size in months. Ignored when min_purchases=1.
                         |    end_threshold_gc: 60 days     // If periodStrategy="limited", represents the period without purchases for an exposure to be considered "finished".
                         |    end_threshold_ngc: 30 days     // If periodStrategy="limited", represents the period without purchases for an exposure to be considered "finished".
                         |    end_delay: 30 days         // Number of periods that we add to the exposure end to delay it (lag).
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
                         |  """.trim.stripMargin

    val expected = defaultConf.copy(
      input = defaultConf.input.copy(
        mco = Some("new/in/path")
      ),
      output = defaultConf.output.copy(
        root = "new/out/path"
      ),
      exposures = defaultConf.exposures.copy(
        minPurchases = 2,
        endThresholdNgc = Some(30.days)
      ),
      drugs = defaultConf.drugs.copy(
        level = PharmacologicalLevel
      )
    )
    //When
    pureconfig.saveConfigAsPropertyFile(ConfigFactory.parseString(stringConfig), Paths.get(tempPath), true)
    val result = FallConfig.load(tempPath, "test")
    //Then
    assert(result == expected)
  }
}

package fr.polytechnique.cmap.cnam.etl.loaders.mlpp.config

import java.nio.file.Paths
import com.typesafe.config.ConfigFactory
import org.scalatest.FlatSpec
import fr.polytechnique.cmap.cnam.etl.config.mlpp.MLPPLoaderConfig
import fr.polytechnique.cmap.cnam.etl.loaders.mlpp.config.MLPPConfig._
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class MLPPConfigSuite extends FlatSpec {

  it should "load the default MLPP config" in {

    //given
    val input = MLPPLoaderConfig.InputPaths(
      patients = Some("src/test/resources/MLPP/patient"),
      outcomes = Some("src/test/resources/MLPP/outcome"),
      exposures = Some("src/test/resources/MLPP/exposure"))

    val output = MLPPLoaderConfig.OutputPaths("target/test/output/featuring")

    val extra = ExtraConfig(maxTimestamp = makeTS(2006, 8, 1))

    val expected = MLPPConfig(
      input = input,
      output = output,
      extra = extra
    )

    //when
    val result = load("", "test")

    //then
    assert(result == expected)
  }


  it should "load the correct config file" in {

    //given
    val test = load("", "test")

    val expected = test.copy(extra = MLPPConfig.ExtraConfig(minTimestamp = makeTS(2006, 1, 1),
      maxTimestamp = makeTS(2006, 2, 1)))

    val tempPath = "target/test.conf"
    val strConf =
      """
        | input {
        |   patients: "src/test/resources/MLPP/patient"
        |   outcomes: "src/test/resources/MLPP/outcome"
        |   exposures: "src/test/resources/MLPP/exposure"
        | }
        |
        | output={
        |   root = "target/test/output/featuring"
        | }
        | extra={
        |   min_timestamp = "2006-01-01"
        |   max_timestamp = "2006-02-01"
        | }
      """.stripMargin
    pureconfig.saveConfigAsPropertyFile(ConfigFactory.parseString(strConf), Paths.get(tempPath), true)

    //when
    val result = MLPPConfig.load(tempPath, "test")

    //then
    assert(result == expected)

  }


}

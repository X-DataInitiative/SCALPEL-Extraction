// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.implicits
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.study.fall.config.FallConfig
import org.apache.spark.sql.functions.lit

class FallMainSuite extends SharedContext {

  "appName" should "return the correct string" in {
    // Given
    val expected = "fall study"

    // When
    val result = FallMain.appName

    // Then
    assert(expected == result)
  }

  "run" should "return None" in {
    val sqlCtx = sqlContext

    //Given
    val params = Map(
      "conf" -> "/src/main/resources/config/fall/default.conf",
      "env" -> "test"
    )

    // When
    val result = FallMain.run(sqlCtx, params)

    // Then
    assert(result.isEmpty)
  }

  "computeExposures" should "return meta data" in {

    //Given
    val fallConfig = FallConfig.load("", "test")

    import implicits.SourceReader
    val sources = Sources.sanitize(sqlContext.readSources(fallConfig.input))
    val expectedOutputPaths = List(
      "target/test/output/drug_purchases/data", "target/test/output/extract_patients/data",
      "target/test/output/filter_patients/data", "target/test/output/exposures/data"
    )
    val expectedOutputTypes = List("dispensations", "patients", "exposures")

    //When
    val result = FallMain.computeExposures(sources, fallConfig)
    val resultOutputPaths = result.map(_.outputPath).toList
    val resultOutputTypes = result.map(_.outputType.toString).toList

    //Then
    assert(expectedOutputPaths.forall(resultOutputPaths.contains))
    assert(expectedOutputTypes.forall(resultOutputTypes.contains))
  }

  "computeOutcomes" should "return meta data" in {

    //Given
    val fallConfig = FallConfig.load("", "test")

    import implicits.SourceReader
    val sources = Sources.sanitize(sqlContext.readSources(fallConfig.input))
    val expectedOutputPaths = List(
      "target/test/output/diagnoses/data", "target/test/output/acts/data",
      "target/test/output/liberal_acts/data", "target/test/output/fractures/data"
    )
    val expectedOutputTypes = List("diagnosis", "acts", "outcomes")

    //When
    val result = FallMain.computeOutcomes(sources, fallConfig)
    val resultOutputPaths = result.map(_.outputPath).toList
    val resultOutputTypes = result.map(_.outputType.toString).toList

    //Then
    assert(expectedOutputPaths.forall(resultOutputPaths.contains))
    assert(expectedOutputTypes.forall(resultOutputTypes.contains))
  }

  "computeHospitalStays" should "return meta data" in {
    //Given
    val fallConfig = FallConfig.load("", "test")

    import implicits.SourceReader
    val sources = Sources.sanitize(sqlContext.readSources(fallConfig.input))
    val expectedOutputPaths = List("target/test/output/extract_hospital_stays/data")
    val expectedOutputTypes = List("hospital stays")

    //When
    val result = FallMain.computeHospitalStays(sources, fallConfig)
    val resultOutputPaths = result.map(_.outputPath).toList
    val resultOutputTypes = result.map(_.outputType.toString).toList

    //Then
    assert(expectedOutputPaths.forall(resultOutputPaths.contains))
    assert(expectedOutputTypes.forall(resultOutputTypes.contains))
  }
}

package fr.polytechnique.cmap.cnam.study.fall

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Event, Outcome}

class FallMainSuite extends SharedContext {

  "getEnv" should "return correct default environment" in {
    // Given
    val args = Map[String, String]()
    val expected = FallMain.TestEnv

    // When
    val result = FallMain.getEnv(args)

    // Then
    assert(result == expected)
  }

  it should "return the Fall environment when specified" in {
    // Given
    val args = Map(("env", "fall"))
    val expected = FallMain.FallEnv

    // When
    val result = FallMain.getEnv(args)

    // Then
    assert(result == expected)
  }

  it should "return the CMAP environment when specified" in {
    // Given
    val args = Map(("env", "cmap"))
    val expected = FallMain.CmapEnv

    // When
    val result = FallMain.getEnv(args)

    // Then
    assert(result == expected)
  }

  "appName" should "return the correct string" in {
    // Given
    val expected = "fall study"

    // When
    val result = FallMain.appName

    // Then
    assert(expected == result)
  }

  "run" should "return correct result" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val expected = sqlCtx.sparkSession.emptyDataset[Event[Outcome]]

    // When
    val result = FallMain
      .run(sqlCtx, Map(("env", "test"))).get
      .as[Event[Outcome]]

    // Then
    assertDSs(result, expected)


  }

}

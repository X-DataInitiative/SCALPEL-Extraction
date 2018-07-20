package fr.polytechnique.cmap.cnam.study.fall

import fr.polytechnique.cmap.cnam.SharedContext

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
}

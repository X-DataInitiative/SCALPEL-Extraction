package fr.polytechnique.cmap.cnam.filtering.cox

import fr.polytechnique.cmap.cnam.SharedContext

/**
  * Created by sathiya on 29/11/16.
  */
class CoxConfigSuite extends SharedContext {

  "summarize" should "correctly return all the default Cox Parameters from the config file" in {
    // Given
    val expectedResult = Map(
      "filterDelayedPatients" -> true,
      "delayedEntriesThreshold" -> 12,
      "followUpMonthsDelay" -> 6,
      "exposureDefinition.minPurchases" -> 2,
      "exposureDefinition.startDelay" -> 3,
      "exposureDefinition.purchasesWindow" -> 6
    )

    // When
    val result = CoxConfig.summarize

    // Then
    println("Result:")
    result.foreach(println)
    println("Expected:")
    expectedResult.foreach(println)
    assert(result == expectedResult)
  }
}

package fr.polytechnique.cmap.cnam.filtering.cox

import fr.polytechnique.cmap.cnam.SharedContext

/**
  * Created by sathiya on 29/11/16.
  */
class CoxConfigSuite extends SharedContext {

  "summarize" should "correctly return all the default Cox Parameters from the config file" in {
    // Given
    val expectedResult =
      "filterDelayedPatients -> true \n" +
      "delayedEntriesThreshold -> 12 \n" +
      "followUpMonthsDelay -> 6 \n" +
      "exposureDefinition.minPurchases -> 2 \n" +
      "exposureDefinition.startDelay -> 3 \n"+
      "exposureDefinition.purchasesWindow -> 6"

    // When
    val result = CoxConfig.toString

    // Then
    println("Result:")
    println(result)
    println("Expected:")
    println(expectedResult)
    assert(result == expectedResult)
  }
}

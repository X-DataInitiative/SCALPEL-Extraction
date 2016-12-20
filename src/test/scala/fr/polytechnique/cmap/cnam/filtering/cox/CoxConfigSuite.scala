package fr.polytechnique.cmap.cnam.filtering.cox

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.filtering.FilteringConfig

/**
  * Created by sathiya on 29/11/16.
  */
class CoxConfigSuite extends SharedContext {

  override def beforeEach(): Unit = {
    super.beforeEach()

    sqlContext.setConf("conf", "")
    val c = FilteringConfig.getClass.getDeclaredConstructor()
    c.setAccessible(true)
    c.newInstance()
    val c2 = CoxConfig.getClass.getDeclaredConstructor()
    c2.setAccessible(true)
    c2.newInstance()
  }

  "toString" should "correctly return all the default Cox Parameters from the config file" in {

    // Given
    val expectedResult =
      "filterDelayedPatients -> true \n" +
      "delayedEntriesThreshold -> 12 \n" +
      "followUpMonthsDelay -> 6 \n" +
      "exposureDefinition.minPurchases -> 2 \n" +
      "exposureDefinition.startDelay -> 3 \n" +
      "exposureDefinition.purchasesWindow -> 6 \n" +
      "exposureDefinition.cumulativeExposureType -> Simple \n"+
      "exposureDefinition.cumulativeExposureWindow -> 1 \n" +
      "exposureDefinition.cumulativeStartThreshold -> 6 \n" +
      "exposureDefinition.cumulativeEndThreshold -> 4"

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

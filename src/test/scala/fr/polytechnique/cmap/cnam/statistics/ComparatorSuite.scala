package fr.polytechnique.cmap.cnam.statistics

import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.statistics.CustomStatistics._

/**
  * Created by sathiya on 12/08/16.
  */
class ComparatorSuite extends Config{

  "Comparator " should "return true" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val centralDF = {
      Seq(
        (1, 1.0),
        (2, 2.0),
        (3, 3.0)
      ).toDF("Key", "Value1")
    }
    val otherDF = {
      Seq(
        (1, "10"),
        (3, "30"),
        (3, "31")
      ).toDF("Key", "Value2")
    }
    val flatDF: DataFrame = { // centralDF.join(otherDF, Seq("Key"), "left_outer")
      Seq(
        (1, 1.0, "10"),
        (2, 2.0, null),
        (3, 3.0, "30"),
        (3, 3.0, "31")
      ).toDF("Key", "Value1", "Value2")
    }

    // When
    val result: Boolean = Comparator.compare(flatDF, centralDF, otherDF)

    // Then
    assert(result == true)
  }

  "Comparator " should "return false" in {
    // Given
    val er_prs_f = sqlContext.read.parquet("src/test/resources/expected/ER_PRS_F.parquet")
    val flatDfPath = "src/test/resources/expected/DCIR.parquet"
    val flatDF = sqlContext.read.parquet(flatDfPath)
    val prsDFCorrectedColumns = Seq("ORB_BSE_NUM", "ORL_BSE_NUM", "ACC_TIE_IND", "BEN_AMA_COD")
    val flatDFCorrectedColumns = Seq("ORB_BSE_NUM", "ORL_BSE_NUM", "ACC_TIE_IND")

    val flatDFCorrected = flatDF.select(flatDFCorrectedColumns.head, flatDFCorrectedColumns.tail:_*)
    val prsDFCorrected = er_prs_f.select(prsDFCorrectedColumns.head, prsDFCorrectedColumns.tail:_*)

    // When
    val result: Boolean = Comparator.compare(flatDFCorrected, prsDFCorrected)

    // Then
    assert(result == false)
  }
}

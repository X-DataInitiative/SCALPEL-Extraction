package fr.polytechnique.cmap.cnam.etl.transform.exposures

import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.transform.exposures.Columns._
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class NonCumulativeWeightAggSuite extends SharedContext {

  "aggregateWeight" should "add a weight column with a constant '1' value" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: DataFrame = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 8, 1)),
      ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(2009, 1, 1))
    ).toDF(PatientID, Category, Value, Start)

    val expected: DataFrame = Seq(
      ("Patient_A", "molecule", "PIOGLITAZONE", makeTS(2008, 8, 1), 1D),
      ("Patient_B", "molecule", "PIOGLITAZONE", makeTS(2009, 1, 1), 1D)
    ).toDF(PatientID, Category, Value, Start, Weight)

    // When
    val instance = new NonCumulativeWeightAgg(input)
    val result = instance.aggregateWeight

    // Then
    assertDFs(expected, result)
 }

}

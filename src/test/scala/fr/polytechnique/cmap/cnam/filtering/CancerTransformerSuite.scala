package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.filtering.CancerTransformer._

/**
  * Created by burq on 09/08/16.
  */
class CancerTransformerSuite extends SharedContext {

  "extractCancer" should "filter out non-cancer lines" in {

    // Given
    val input: DataFrame = sqlContext.read.load("src/test/resources/expected/IR_IMB_R.parquet")

    // When
    val result: DataFrame = input.extractCancer()

    // Then
    assert(result.count == 1)
  }

  "joinWithPatient" should "add patient info to the three lines" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: DataFrame = sqlContext.read.load("src/test/resources/expected/IR_IMB_R.parquet")
    val patient: DataFrame = Seq(
      ("Patient_02", "2")
    ).toDF("PatientID", "Gender")

    // When
    val result = input.joinWithPatient(patient)

    // Then
    assert(result.columns.contains("PatientID"))
    assert(result.columns.contains("Gender"))
    assert(result.select(col("Gender").cast("int")).collect.map(_.getInt(0)) === Array(2,2,2))
  }

}

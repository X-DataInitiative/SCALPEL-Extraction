package fr.polytechnique.cmap.cnam.filtering

import java.sql.Timestamp

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.filtering.CancerTransformer._
import fr.polytechnique.cmap.cnam.filtering.Sources
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames._

/**
  * Created by burq on 09/08/16.
  */
class CancerTransformerSuite extends SharedContext {

  "extractCancer" should "filter out non-cancer lines" in {

    // Given
    val input: DataFrame = sqlContext.read.load("src/test/resources/expected/IR_IMB_R.parquet")

    // When
    val result: DataFrame = input.extractCancer

    // Then
    assert(result.count == 1)
  }

  "transform" should "return a pretty Dataset" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: DataFrame = sqlContext.read.load("src/test/resources/expected/IR_IMB_R.parquet")
    val source = new Sources(irImb = Some(input))
    val expected = Seq(
      Event(
        patientID = "Patient_02",
        category = "disease",
        eventId = "C67",
        weight = 1,
        start = Timestamp.valueOf("2006-03-13 00:00:00"),
        end = None
      )
    ).toDF

    // When
    val result = CancerTransformer.transform(source)

    // Then
    assert(result.toDF === expected)

  }
}
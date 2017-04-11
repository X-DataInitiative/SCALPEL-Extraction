package fr.polytechnique.cmap.cnam.etl.events.acts

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.util.functions._
import org.apache.spark.sql.DataFrame

class McoActsSuite extends SharedContext{

  "filterActs" should "filter the right lines" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: DataFrame = Seq(
      ("george", "Z51123", makeTS(2011, 12, 12)),
      ("pierre", "ABCDEFFG", makeTS(2011, 12, 12))
    ).toDF("patientID", "code", "event_date")

    val acts: List[String] = List(
      "Z511"
    )

    val expected: DataFrame = Seq(
      ("george", "Z51123", makeTS(2011, 12, 12))
    ).toDF("patientID", "code", "event_date")

    // When
    import McoActs.ActDF
    val result = input.filterActs(acts)

    // Then
    assertDFs(result, expected)
  }

  it should "return an empty dataset when no acts are specified" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: DataFrame = Seq(
      ("george", "Z51123", makeTS(2011, 12, 12)),
      ("pierre", "ABCDEFFG", makeTS(2011, 12, 12))
    ).toDF("patientID", "code", "event_date")

    val acts: List[String] = List()

    // When
    import McoActs.ActDF
    val result = input.filterActs(acts)

    // Then
    assert(result.rdd.isEmpty())
  }

  "extract" should "return the correct dataframe" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val config = ExtractionConfig.init().copy(
      dcirActCodes = List("Z511")
    )

    val mco: DataFrame = Seq(
      ("george", "Z51123", Some(12), Some(2011), 11,
        Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12))),
      ("pierre", "ABCDEFFG", Some(12), Some(2011), 11,
        Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12)))
    ).toDF("NUM_ENQ", "MCO_A.CDC_ACT", "MCO_B.SOR_MOI",
      "MCO_B.SOR_ANN", "MCO_B.SEJ_NBJ", "ENT_DAT", "SOR_DAT")

    val expected: DataFrame = Seq(
      HospitalizedAct("george", "Z51123", makeTS(2011, 12, 1))
    ).toDF

    // When
    val result = McoActs.extract(config, mco)

    // Then
    assertDFs(result.toDF, expected)
  }
}

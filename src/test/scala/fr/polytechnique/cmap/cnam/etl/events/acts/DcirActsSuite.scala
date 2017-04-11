package fr.polytechnique.cmap.cnam.etl.events.acts

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.util.functions._
import org.apache.spark.sql.DataFrame

class DcirActsSuite extends SharedContext{

  "filterActs" should "filter the right lines" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: DataFrame = Seq(
      ("george", "Z51123", makeTS(2011, 12, 12)),
      ("pierre", "TEST", makeTS(2011, 12, 12))
    ).toDF("patientID", "code", "eventDate")

    val acts: List[String] = List(
      "Z511"
    )

    val expected: DataFrame = Seq(
      ("george", "Z51123", makeTS(2011, 12, 12))
    ).toDF("patientID", "code", "eventDate")

    // When
    import DcirActs.ActDF
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
    import DcirActs.ActDF
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

    val dcir: DataFrame = Seq(
      ("george", "Z51123", makeTS(2011, 12, 12)),
      ("pierre", "TEST", makeTS(2011, 12, 12))
    ).toDF("NUM_ENQ", "ER_CAM_F.CAM_PRS_IDE", "EXE_SOI_DTD")

    val expected: DataFrame = Seq(
      NonHospitalizedAct("george", "Z51123", makeTS(2011, 12, 12))
    ).toDF

    // When
    val result = DcirActs.extract(config, dcir)

    // Then
    assertDFs(result.toDF, expected)
  }
}

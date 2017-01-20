package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.functions.makeTS
import fr.polytechnique.cmap.cnam.filtering.InfarctusTransformer._
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames

trait fakeInfarctusFixture extends SharedContext {

  def fakeMcoData: DataFrame = {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    Seq(
      ("HasInfarctus1", Some("I21"),
        Some(12), Some(2011), 11, Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12))
      ),
      ("HasInfarctus2", Some("I22"),
        Some(12), Some(2011), 11, null, Some(makeTS(2011, 12, 12))
      ),
      ("MustBeDropped1", null,
        Some(1), Some(2010), 31, Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12))
      ),
      ("MustBeDropped2", Some("TOTO"),
        Some(1), Some(2010), 31, Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12))
      )
    ).toDF("NUM_ENQ", "MCO_B.DGN_PAL",
      "MCO_B.SOR_MOI", "MCO_B.SOR_ANN", "MCO_B.SEJ_NBJ", "ENT_DAT", "SOR_DAT")
  }

}
class InfarctusTransformerSuite extends fakeInfarctusFixture {

  "extractDisease" should "return correct result" in {
    // Given
    val input = fakeMcoData

    // When
    val output = input.select(mcoInputColumns:_*).extractDisease

    // Then
    assert(output.where(col("patientID") contains "MustBeDropped").count == 0)
  }

  "transform" should "return the correct result" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val data = fakeMcoData
    val input = new Sources(pmsiMco=Some(data))
    val expected = Seq(
      Event("HasInfarctus1", "disease", "infarctus", 1, makeTS(2011, 12, 1), None),
      Event("HasInfarctus2", "disease", "infarctus", 1, makeTS(2011, 12, 1), None)
    ).toDF

    // When
    val output = InfarctusTransformer.transform(input)

    // Then
    import RichDataFrames._
    assert(output.toDF === expected)
  }

}

package fr.polytechnique.cmap.cnam.etl.old_root

import java.sql.Timestamp
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions._

/**
  * Created by burq on 29/09/16.
  */
class DcirActTransformerSuite extends SharedContext {

  "filterActs" should "return event with correct acts" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val input = Seq(
      ("john", "Z511", makeTS(2016, 10, 1)),
      ("george", "toto", makeTS(2016, 10, 1)),
      ("john", null, makeTS(2016, 10, 1))
    ).toDF("patientID", "actCode", "eventDate")

    val expected = Seq(
      ("john", "Z511", makeTS(2016, 10, 1))
    ).toDF("patientID", "actCode", "eventDate")


    // When
    import fr.polytechnique.cmap.cnam.etl.old_root.DcirActTransformer._
    val result = input.filterActs

    // Then
    assertDFs(result, expected)
  }

  "transform" should "return correct result" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val dcir = Seq(
      ("john", "Z511", makeTS(2016, 10, 1)),
      ("george", "toto", makeTS(2016, 10, 1)),
      ("john", null, makeTS(2016, 10, 1))
    ).toDF("NUM_ENQ", "ER_CAM_F__CAM_PRS_IDE", "EXE_SOI_DTD")

    val expected = Seq(
      ("john", "act", "radiotherapy", 1.0, makeTS(2016, 10, 1), null.asInstanceOf[Timestamp])
    ).toDF("patientID", "category", "eventId", "weight", "start", "end")

    val input = new Sources(dcir = Some(dcir))

    // When
    val result = DcirActTransformer.transform(input)

    // Then
    assertDFs(result.toDF, expected)

  }

}

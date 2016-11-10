package fr.polytechnique.cmap.cnam.filtering

import java.sql.Timestamp

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames
import fr.polytechnique.cmap.cnam.utilities.functions._

/**
  * Created by burq on 29/09/16.
  */
class DcirActTransformerSuite extends SharedContext {

  "filterActs" should "return event with correct acts" in {
    val sqlCtx = sqlContext
    import  sqlCtx.implicits._
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
    import DcirActTransformer._
    val result = input.filterActs

    // Then
    import RichDataFrames._
    assert(result === expected)
  }

  "transform" should "return correct result" in {
    val sqlCtx = sqlContext
    import  sqlCtx.implicits._
    // Given
    val dcir = Seq(
      ("john", "Z511", makeTS(2016, 10, 1)),
      ("george", "toto", makeTS(2016, 10, 1)),
      ("john", null, makeTS(2016, 10, 1))
    ).toDF("NUM_ENQ", "ER_CAM_F.CAM_PRS_IDE", "EXE_SOI_DTD")

    val expected = Seq(
      ("john", "act", "radiotherapy", 1.0, makeTS(2016, 10, 1), null.asInstanceOf[Timestamp])
    ).toDF("patientID", "category", "eventId", "weight", "start", "end")

    val input = new Sources(dcir = Some(dcir))

    // When
    val result = DcirActTransformer.transform(input)

    // Then
    import RichDataFrames._
    assert(result.toDF === expected)

  }

}
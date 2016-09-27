package fr.polytechnique.cmap.cnam.filtering

import java.sql.Timestamp

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames
import fr.polytechnique.cmap.cnam.utilities.functions._



class BladderCancerTransformerSuite extends SharedContext{

  "extractNarrowCancer" should "capture the event when code found in Diagnostic principal" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val input = Seq(
      ("JohnDoe", Some("C67*"), Some("TOTO"), Some("TOTO")),
      ("I m fine", null, null, Some("TOTO"))
    ).toDF("patientID", "DP", "DR", "DAS")

    // When
    import BladderCancerTransformer._
    val result = input.extractNarrowCancer


    // Then
    assert(result.count()==1)
  }

  "extractNarrowCancer" should "capture the event when code found in Diagnostic relié" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val input = Seq(
      ("JohnDoe", Some("TOTO"), Some("C67*"), Some("TOTO")),
      ("I m fine", null, null, Some("TOTO"))
    ).toDF("patientID", "DP", "DR", "DAS")

    // When
    import BladderCancerTransformer._
    val result = input.extractNarrowCancer


    // Then
    assert(result.count()==1)
  }

  "extractNarrowCancer" should "capture the event when code combination found with DP" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val input = Seq(
      ("C77 & C67", Some("C77*"), Some("TOTO"), Some("C67*")),
      ("C78 & C67", Some("C78*"), Some("TOTO"), Some("C67*")),
      ("C79 & C67", Some("C79*"), Some("TOTO"), Some("C67*")),
      ("I m fine", null, null, Some("TOTO"))
    ).toDF("patientID", "DP", "DR", "DAS")

    // When
    import BladderCancerTransformer._
    val result = input.extractNarrowCancer


    // Then
    assert(result.count()==3)
  }

  "extractNarrowCancer" should "capture the event when code combination found with DR" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val input = Seq(
      ("C67 & C77", Some("TOTO"), Some("C77*"), Some("C67*")),
      ("C67 & C78", Some("TOTO"), Some("C78*"), Some("C67*")),
      ("C67 & C79", Some("TOTO"), Some("C79*"), Some("C67*")),
      ("I m fine", null, null, Some("TOTO"))
    ).toDF("patientID", "DP", "DR", "DAS")

    // When
    import BladderCancerTransformer._
    val result = input.extractNarrowCancer


    // Then
    assert(result.count()==3)
  }


  "extractNarrowCancer" should "not take C67 alone in DAS position" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val input = Seq(
      ("I'm a bit sick", Some("TOTO"), Some("TUTU"), Some("C67*"))
    ).toDF("patientID", "DP", "DR", "DAS")

    // When
    import BladderCancerTransformer._
    val result = input.extractNarrowCancer


    // Then
    assert(result.count()==0)
  }

  "withDelta" should "return columns with delta for next and previous years" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val input = Seq(
      ("robert", "event1", makeTS(2016, 1, 1), null),
      ("robert", "event2", makeTS(2016, 3, 1), makeTS(2016, 5, 1)),
      ("robert", "event3", makeTS(2016, 7, 1), null),
      ("robert", "event4", makeTS(2016, 12,1), null)
    ).toDF("patientID", "eventId", "start", "end")

    val expected = Seq(
      ("event1", 2.0, null.asInstanceOf[Double]),
      ("event2", 2.0, 2.0),
      ("event3", 5.0, 2.0),
      ("event4", null.asInstanceOf[Double], 5.0)
    ).toDF("eventId", "nextDelta", "previousDelta")

    // When
    import BladderCancerTransformer._
    val result = input.withDelta.select("eventId", "nextDelta", "previousDelta")

    // Then
    import RichDataFrames._
    assert(result === expected)

  }

  "withNextType" should "return the next eventId" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val input = Seq(
      ("robert", "event1", makeTS(2016, 1, 1)),
      ("robert", "event2", makeTS(2016, 3, 1)),
      ("robert", "event3", makeTS(2016, 7, 1))
    ).toDF("patientID", "eventId", "start")

    val expected = Seq(
      ("event1", "event2", null),
      ("event2", "event3", "event1"),
      ("event3", null, "event2")
    ).toDF("eventId", "nextType", "previousType")

    // When
    import BladderCancerTransformer._
    val result = input.withNextType.select("eventId", "nextType", "previousType")

    // Then
    import RichDataFrames._
    assert(result === expected)

  }

  "filterBladderCancer" should "return event BladderCancer with radiotherapy close after" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val input = Seq(
      ("robert", "bladderCancer", makeTS(2016, 8, 1), null.asInstanceOf[Timestamp], "radiotherapy", 2.0),
      ("robert", "radiotherapy", makeTS(2016, 10, 1), null.asInstanceOf[Timestamp], null, null.asInstanceOf[Double])
    ).toDF("patientID", "eventId", "start", "end", "nextType", "nextDelta")

    val expected = Seq(
      ("robert", "bladderCancer", makeTS(2016, 8, 1), null.asInstanceOf[Timestamp], "radiotherapy", 2.0)
    ).toDF("patientID", "eventId", "start", "end", "nextType", "nextDelta")

    // When
    import BladderCancerTransformer._
    val result = input.filterBladderCancer

    // Then
    import RichDataFrames._
    assert(result === expected)

  }

  "filterBladderCancer" should "return event BladderCancer with radiotherapy close before" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val input = Seq(
      ("robert", "radiotherapy", makeTS(2016, 1, 1), null.asInstanceOf[Timestamp],
        "bladderCancer", 2.0, null, null.asInstanceOf[Double]),
      ("robert", "bladderCancer", makeTS(2016, 3, 1), null.asInstanceOf[Timestamp],
        "bladderCancer", 5.0, "radiotherapy", 2.0),
      ("robert", "bladderCancer", makeTS(2016, 8, 1), null.asInstanceOf[Timestamp],
        null, null.asInstanceOf[Double], "bladderCancer", 5.0)
    ).toDF("patientID", "eventId", "start", "end",
      "nextType", "nextDelta", "previousType", "previousDelta")

    val expected = Seq(
      ("robert", "bladderCancer", makeTS(2016, 3, 1), null.asInstanceOf[Timestamp],
        "bladderCancer", 5.0, "radiotherapy", 2.0)
    ).toDF("patientID", "eventId", "start", "end",
      "nextType", "nextDelta", "previousType", "previousDelta")

    // When
    import BladderCancerTransformer._
    val result = input.filterBladderCancer

    // Then
    import RichDataFrames._
    assert(result === expected)

  }



  "transform" should "return the correct result" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val mco = Seq(
      ("Patient1", Some("C67*"), Some("TUTU"), Some("TATA"), Some(12), Some(2011),
        11, Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12))),
      ("Patient2", Some("TOTO"), Some("C78*"), Some("C67*"), Some(12), Some(2011),
        11, null, Some(makeTS(2011, 12, 12))),
      ("Patient3", Some("TOTO"), Some("TUTU"), Some("TATA"), Some(12), Some(2011),
        11, null, null)
    ).toDF("NUM_ENQ", "MCO_B.DGN_PAL", "MCO_B.DGN_REL", "MCO_D.ASS_DGN", "MCO_B.SOR_MOI", "MCO_B.SOR_ANN",
      "MCO_B.SEJ_NBJ", "ENT_DAT", "SOR_DAT")

    val dcir = Seq(
      ("Patient1", "Z511", makeTS(2012, 2, 1)),
      ("Patient2", "7511", makeTS(2016, 10, 1)),
      ("Patient3", null, makeTS(2016, 10, 1))
    ).toDF("NUM_ENQ", "ER_CAM_F.CAM_PRS_IDE", "EXE_SOI_DTD")

    val input = new Sources(pmsiMco=Some(mco), dcir=Some(dcir))
    val expected = Seq(
      Event("Patient1", "disease", "bladderCancer", 1, makeTS(2011, 12, 1), None)
    ).toDF

    // When
    val output = BladderCancerTransformer.transform(input)

    // Then
    import RichDataFrames._
    output.show()
    expected.show()
    assert(output.toDF === expected)
  }
}

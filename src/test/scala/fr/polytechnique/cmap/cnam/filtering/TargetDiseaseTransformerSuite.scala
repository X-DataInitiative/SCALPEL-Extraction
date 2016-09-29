package fr.polytechnique.cmap.cnam.filtering

import java.sql.Timestamp
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames
import fr.polytechnique.cmap.cnam.utilities.functions._

class TargetDiseaseTransformerSuite extends SharedContext {

  "withDelta" should "return columns with delta for next and previous years" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("robert", "event1", makeTS(2016, 1, 1), None),
      ("robert", "event2", makeTS(2016, 3, 1), Some(makeTS(2016, 5, 1))),
      ("robert", "event3", makeTS(2016, 7, 1), None),
      ("robert", "event4", makeTS(2016, 12,1), None)
    ).toDF("patientID", "eventId", "start", "end")

    val expected = Seq(
      ("event1", Some(2.0), None),
      ("event2", Some(2.0), Some(2.0)),
      ("event3", Some(5.0), Some(2.0)),
      ("event4", None, Some(5.0))
    ).toDF("eventId", "nextDelta", "previousDelta")

    // When
    import TargetDiseaseTransformer._
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
      ("event1", Some("event2"), None),
      ("event2", Some("event3"), Some("event1")),
      ("event3", None, Some("event2"))
    ).toDF("eventId", "nextType", "previousType")

    // When
    import TargetDiseaseTransformer._
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
      ("robert", "bladderCancer", makeTS(2016, 8, 1), None: Option[Timestamp],
        Some("radiotherapy"), 2.0, None, None),
      ("robert", "radiotherapy", makeTS(2016, 10, 1), None: Option[Timestamp],
        None, 0.0, Some("bladderCancer"), Some(2.0))
    ).toDF("patientID", "eventId", "start", "end",
      "nextType", "nextDelta", "previousType", "previousDelta")

    val expected = Seq(
      ("robert", "bladderCancer", makeTS(2016, 8, 1), None: Option[Timestamp],
        "radiotherapy", 2.0, None: Option[String], None: Option[Double])
    ).toDF("patientID", "eventId", "start", "end",
      "nextType", "nextDelta", "previousType", "previousDelta")

    // When
    import TargetDiseaseTransformer._
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
      ("robert", "radiotherapy", makeTS(2016, 1, 1), None: Option[Timestamp],
        Some("bladderCancer"), Some(2.0), None, None),
      ("robert", "bladderCancer", makeTS(2016, 3, 1), None: Option[Timestamp],
        Some("bladderCancer"), Some(5.0), Some("radiotherapy"), Some(2.0)),
      ("robert", "bladderCancer", makeTS(2016, 8, 1), None: Option[Timestamp],
        None, None, Some("bladderCancer"), Some(5.0))
    ).toDF("patientID", "eventId", "start", "end",
      "nextType", "nextDelta", "previousType", "previousDelta")

    val expected = Seq(
      ("robert", "bladderCancer", makeTS(2016, 3, 1), None: Option[Timestamp],
        "bladderCancer", 5.0, "radiotherapy", 2.0)
    ).toDF("patientID", "eventId", "start", "end",
      "nextType", "nextDelta", "previousType", "previousDelta")

    // When
    import TargetDiseaseTransformer._
    val result = input.filterBladderCancer

    // Then
    import RichDataFrames._
    assert(result === expected)
  }

  /* TODO: This test must be updated
  "transform" should "return the correct result" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val initialData = Seq(
      ("Patient1", Some("Z511"), "C67", None: Option[String], Some("JDFA014"), Some(12), Some(2011), 11,
        Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12))),
      ("Patient2", None, "C67", None: Option[String], Some("JDFA003"), Some(12), Some(2011), 11,
        None, Some(makeTS(2011, 12, 12))),
      ("Patient3", Some("..."), "C67", None: Option[String], Some("..."), Some(12), Some(2011), 11,
        None, None)
    ).toDF("NUM_ENQ", "MCO_B.DGN_PAL", "MCO_B.DGN_REL", "MCO_D.ASS_DGN", "MCO_A.CDC_ACT",
      "MCO_B.SOR_MOI", "MCO_B.SOR_ANN", "MCO_B.SEJ_NBJ", "ENT_DAT", "SOR_DAT")

    import McoActTransformer.GHSColumnNames
    val data = GHSColumnNames.foldLeft(initialData)(
      (df, colName) => df.withColumn(colName, lit(null).cast(IntegerType))
    )

    val input = new Sources(pmsiMco=Some(data))
    val expected = Seq(
      Event("Patient1", "act", "mco", 1.0, makeTS(2011, 12, 1), None),
      Event("Patient2", "act", "mco", 1.0, makeTS(2011, 12, 1), None)
    ).toDF

    // When
    val output = TargetDiseaseTransformer.transform(input)

    // Then
    import RichDataFrames._
    output.show()
    expected.show()
    assert(output.toDF === expected)
  }
  */
}

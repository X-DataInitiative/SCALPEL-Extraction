package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames
import fr.polytechnique.cmap.cnam.utilities.functions._

class McoActTransformerSuite extends SharedContext {

  import McoActTransformer.GHSColumnNames

  "filterActs" should "return correct events when DP = Z510 or Z511 " in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val initialInput = Seq(
      ("Patient1", Some("Z511"), "C67", None: Option[String], Some("...")),
      ("Patient1", None, "C67", None: Option[String], None),
      ("Patient2", Some("Z510"), "C67", None: Option[String], Some("...")),
      ("Patient2", None, "C67", None: Option[String], Some("...")),
      ("Patient2", Some("..."), "C67", None: Option[String], None),
      ("Patient3", Some("Z510"), "C67", None: Option[String], None),
      ("Patient3", Some("Z510"), "C67", None: Option[String], None),
      ("Patient4", None, "C67", None: Option[String], None)
    ).toDF("patientID", "DP", "DR", "DAS", "CCAM")

    val input = GHSColumnNames.foldLeft(initialInput)(
      (df, colName) => df.withColumn(colName, lit(null).cast(IntegerType))
    )

    val expected = Seq(
      ("Patient1", Some("Z511"), "C67", None: Option[String], Some("...")),
      ("Patient2", Some("Z510"), "C67", None: Option[String], Some("...")),
      ("Patient3", Some("Z510"), "C67", None: Option[String], None),
      ("Patient3", Some("Z510"), "C67", None: Option[String], None)
    ).toDF("patientID", "DP", "DR", "DAS", "CCAM")

    // When
    import McoActTransformer._
    val result = input.filterActs.select("patientID", "DP", "DR", "DAS", "CCAM")

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  it should "return correct events when CCAM in specific acts" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val initialInput = Seq(
      ("Patient1", Some("..."), "C67", None: Option[String], Some("JDFA015")),
      ("Patient1", None, "C67", None: Option[String], Some("JDFC023")),
      ("Patient2", None, "C67", None: Option[String], Some("JDLD002")),
      ("Patient2", None, "C67", None: Option[String], None),
      ("Patient3", None, "C67", None: Option[String], Some("...")),
      ("Patient3", None, "C67", None: Option[String], Some("JDFA008")),
      ("Patient4", None, "C67", None: Option[String], None)
    ).toDF("patientID", "DP", "DR", "DAS", "CCAM")

    val input = GHSColumnNames.foldLeft(initialInput)(
      (df, colName) => df.withColumn(colName, lit(null).cast(IntegerType))
    )

    val expected = Seq(
      ("Patient1", Some("..."), "C67", None: Option[String], Some("JDFA015")),
      ("Patient1", None, "C67", None: Option[String], Some("JDFC023")),
      ("Patient2", None, "C67", None: Option[String], Some("JDLD002")),
      ("Patient3", None, "C67", None: Option[String], Some("JDFA008"))
    ).toDF("patientID", "DP", "DR", "DAS", "CCAM")

    // When
    import McoActTransformer._
    val result = input.filterActs.select("patientID", "DP", "DR", "DAS", "CCAM")

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  it should "return correct events when one of the GHS columns has a value larger than 0" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val columnNameToTest = "MCO_B.GHS_9515_ACT"
    val columnToTest = col("`MCO_B.GHS_9515_ACT`")

    // Given
    val initialInput = Seq(
      ("Patient1", Some("..."), "C67", None: Option[String], None, Some(1)),
      ("Patient1", None, "C67", None: Option[String], Some("..."), Some(2)),
      ("Patient2", None, "C67", None: Option[String], None, Some(0)),
      ("Patient2", None, "C67", None: Option[String], None, None),
      ("Patient3", None, "C67", None: Option[String], None, Some(3)),
      ("Patient3", None, "C67", None: Option[String], None, Some(4)),
      ("Patient4", None, "C67", None: Option[String], None, None)
    ).toDF("patientID", "DP", "DR", "DAS", "CCAM", columnNameToTest)

    val input = (GHSColumnNames - columnNameToTest).foldLeft(initialInput)(
      (df, colName) => df.withColumn(colName, lit(null).cast(IntegerType))
    )

    val expected = Seq(
      ("Patient1", Some("..."), "C67", None: Option[String], None, Some(1)),
      ("Patient1", None, "C67", None: Option[String], Some("..."), Some(2)),
      ("Patient3", None, "C67", None: Option[String], None, Some(3)),
      ("Patient3", None, "C67", None: Option[String], None, Some(4))
    ).toDF("patientID", "DP", "DR", "DAS", "CCAM", "GHS")

    // When
    import McoActTransformer._
    val result = input.filterActs.select(col("patientID"), col("DP"), col("DR"), col("DAS"), col("CCAM"), columnToTest.as("GHS"))

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "transform" should "return correct result" in {
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

    val data = GHSColumnNames.foldLeft(initialData)(
      (df, colName) => df.withColumn(colName, lit(null).cast(IntegerType))
    )

    val input = new Sources(pmsiMco=Some(data))
    val expected = Seq(
      Event("Patient1", "disease", "targetDisease", 1.0, makeTS(2011, 12, 1), None),
      Event("Patient2", "disease", "targetDisease", 1.0, makeTS(2011, 12, 1), None)
    ).toDF

    // When
    val output = McoActTransformer.transform(input)

    // Then
    import RichDataFrames._
    output.show()
    expected.show()
    assert(output.toDF === expected)
  }
}

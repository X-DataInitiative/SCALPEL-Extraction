package fr.polytechnique.cmap.cnam.etl.old_root

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions._

class McoActTransformerSuite extends SharedContext {

  import fr.polytechnique.cmap.cnam.etl.old_root.McoActTransformer.GHSColumnNames

  "filterBladderCancers" should "capture the event when code found in Diagnostic principal" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val input = Seq(
      ("JohnDoe", Some("C67*"), Some("TOTO"), Some("TOTO")),
      ("I m fine", None, None, Some("TOTO"))
    ).toDF("patientID", "DP", "DR", "DAS")

    // When
    import fr.polytechnique.cmap.cnam.etl.old_root.McoActTransformer.ActDF
    val result = input.filterBladderCancers


    // Then
    assert(result.count()==1)
  }

  it should "capture the event when code found in Diagnostic relié" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val input = Seq(
      ("JohnDoe", Some("TOTO"), Some("C67*"), Some("TOTO")),
      ("I m fine", None, None, Some("TOTO"))
    ).toDF("patientID", "DP", "DR", "DAS")

    // When
    import fr.polytechnique.cmap.cnam.etl.old_root.McoActTransformer.ActDF
    val result = input.filterBladderCancers


    // Then
    assert(result.count()==1)
  }

  it should "capture the event when code combination found with DP" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val input = Seq(
      ("C77 & C67", Some("C77*"), Some("TOTO"), Some("C67*")),
      ("C78 & C67", Some("C78*"), Some("TOTO"), Some("C67*")),
      ("C79 & C67", Some("C79*"), Some("TOTO"), Some("C67*")),
      ("I m fine", None, None, Some("TOTO"))
    ).toDF("patientID", "DP", "DR", "DAS")

    // When
    import fr.polytechnique.cmap.cnam.etl.old_root.McoActTransformer.ActDF
    val result = input.filterBladderCancers


    // Then
    assert(result.count()==3)
  }

  it should "capture the event when code combination found with DR" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val input = Seq(
      ("C67 & C77", Some("TOTO"), Some("C77*"), Some("C67*")),
      ("C67 & C78", Some("TOTO"), Some("C78*"), Some("C67*")),
      ("C67 & C79", Some("TOTO"), Some("C79*"), Some("C67*")),
      ("I m fine", None, None, Some("TOTO"))
    ).toDF("patientID", "DP", "DR", "DAS")

    // When
    import fr.polytechnique.cmap.cnam.etl.old_root.McoActTransformer.ActDF
    val result = input.filterBladderCancers


    // Then
    assert(result.count()==3)
  }

  it should "not take C67 alone in DAS position" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val input = Seq(
      ("I'm a bit sick", Some("TOTO"), Some("TUTU"), Some("C67*"))
    ).toDF("patientID", "DP", "DR", "DAS")

    // When
    import fr.polytechnique.cmap.cnam.etl.old_root.McoActTransformer.ActDF
    val result = input.filterBladderCancers


    // Then
    assert(result.count()==0)
  }

  "filterActs" should "return correct events when DP = Z510 or Z511 " in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val initialInput = Seq(
      ("Patient1", Some("Z511"), Some("...")),
      ("Patient1", None, None),
      ("Patient2", Some("Z510"), Some("...")),
      ("Patient2", None, Some("...")),
      ("Patient2", Some("..."), None),
      ("Patient3", Some("Z510"), None),
      ("Patient3", Some("Z510"), None),
      ("Patient4", None, None)
    ).toDF("patientID", "DP", "CCAM")

    val input = GHSColumnNames.foldLeft(initialInput)(
      (df, colName) => df.withColumn(colName, lit(null).cast(IntegerType))
    )

    val expected = Seq(
      ("Patient1", Some("Z511"), Some("...")),
      ("Patient2", Some("Z510"), Some("...")),
      ("Patient3", Some("Z510"), None),
      ("Patient3", Some("Z510"), None)
    ).toDF("patientID", "DP", "CCAM")

    // When
    import fr.polytechnique.cmap.cnam.etl.old_root.McoActTransformer.ActDF
    val result = input.filterActs.select("patientID", "DP", "CCAM")

    // Then
    assertDFs(result, expected)
  }

  it should "return correct events when CCAM in specific acts" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val initialInput = Seq(
      ("Patient1", Some("..."), Some("JDFA015")),
      ("Patient1", None, Some("JDFC023")),
      ("Patient2", None, Some("JDLD002")),
      ("Patient2", None, None),
      ("Patient3", None, Some("...")),
      ("Patient3", None, Some("JDFA008")),
      ("Patient4", None, None)
    ).toDF("patientID", "DP", "CCAM")

    val input = GHSColumnNames.foldLeft(initialInput)(
      (df, colName) => df.withColumn(colName, lit(null).cast(IntegerType))
    )

    val expected = Seq(
      ("Patient1", Some("..."), Some("JDFA015")),
      ("Patient1", None, Some("JDFC023")),
      ("Patient2", None, Some("JDLD002")),
      ("Patient3", None, Some("JDFA008"))
    ).toDF("patientID", "DP", "CCAM")

    // When
    import fr.polytechnique.cmap.cnam.etl.old_root.McoActTransformer.ActDF
    val result = input.filterActs.select("patientID", "DP", "CCAM")

    // Then
    assertDFs(result, expected)
  }

  it should "return correct events when one of the GHS columns has a value larger than 0" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val columnNameToTest = "MCO_B__GHS_9515_ACT"
    val columnToTest = col("MCO_B__GHS_9515_ACT")

    // Given
    val initialInput = Seq(
      ("Patient1", Some("..."), None, Some(1)),
      ("Patient1", None, Some("..."), Some(2)),
      ("Patient2", None, None, Some(0)),
      ("Patient2", None, None, None),
      ("Patient3", None, None, Some(3)),
      ("Patient3", None, None, Some(4)),
      ("Patient4", None, None, None)
    ).toDF("patientID", "DP", "CCAM", columnNameToTest)

    val input = (GHSColumnNames - columnNameToTest).foldLeft(initialInput)(
      (df, colName) => df.withColumn(colName, lit(null).cast(IntegerType))
    )

    val expected = Seq(
      ("Patient1", Some("..."), None, Some(1)),
      ("Patient1", None, Some("..."), Some(2)),
      ("Patient3", None, None, Some(3)),
      ("Patient3", None, None, Some(4))
    ).toDF("patientID", "DP", "CCAM", "GHS")

    // When
    import fr.polytechnique.cmap.cnam.etl.old_root.McoActTransformer.ActDF
    val result = input.filterActs.select(col("patientID"), col("DP"), col("CCAM"), columnToTest.as("GHS"))

    // Then
    assertDFs(result, expected)
  }

  "transform" should "return the correct results for a known input" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val initialData = Seq(
      ("Patient1", Some("Z511"), "C67", None: Option[String], Some("JDFA014"), Some(12), Some(2011), 11,
        Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12))),
      ("Patient2", None, "C67", None: Option[String], Some("JDFA003"), Some(12), Some(2011), 11,
        None, Some(makeTS(2011, 12, 12))),
      ("Patient3", Some("..."), "C67", None: Option[String], Some("..."), Some(12), Some(2011), 11,
        None, Some(makeTS(2011, 12, 12))),
      ("Patient3", Some("..."), "C67", None: Option[String], Some("..."), Some(12), Some(2011), 11,
        None, Some(makeTS(2011, 12, 12)))
    ).toDF("NUM_ENQ", "MCO_B__DGN_PAL", "MCO_B__DGN_REL", "MCO_D__ASS_DGN", "MCO_A__CDC_ACT",
      "MCO_B__SOR_MOI", "MCO_B__SOR_ANN", "MCO_B__SEJ_NBJ", "ENT_DAT", "SOR_DAT")

    val data = GHSColumnNames.foldLeft(initialData)(
      (df, colName) => df.withColumn(colName, lit(null).cast(IntegerType))
    )

    val input = new Sources(pmsiMco=Some(data))
    val expected = Seq(
      Event("Patient1", "disease", "bladderCancer", 1.0, makeTS(2011, 12, 1), None),
      Event("Patient2", "disease", "bladderCancer", 1.0, makeTS(2011, 12, 1), None),
      Event("Patient3", "disease", "bladderCancer", 1.0, makeTS(2011, 12, 1), None),
      Event("Patient1", "disease", "targetDisease", 1.0, makeTS(2011, 12, 1), None),
      Event("Patient2", "disease", "targetDisease", 1.0, makeTS(2011, 12, 1), None)
    ).toDF

    // When
    val output = McoActTransformer.transform(input)

    // Then
    assertDFs(output.toDF, expected)
 }
}

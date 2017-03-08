package fr.polytechnique.cmap.cnam.etl.old_root

import java.sql.Timestamp
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.RichDataFrames

/**
  * Created by burq on 13/09/16.
  */
class McoPatientTransformerSuite extends SharedContext {

  "getDeathDates" should "collect death dates correctly from flat MCO" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: DataFrame = Seq(
      ("Patient_01", 1, 2, 1985),
      ("Patient_02", 9, 3, 1986)
    ).toDF("patientID", "SOR_MOD", "SOR_MOI", "SOR_ANN")

    val expected: DataFrame = Seq(
      ("Patient_02", Timestamp.valueOf("1986-03-01 00:00:00"))
    ).toDF("patientID", "deathDate")

    // When
    import fr.polytechnique.cmap.cnam.etl.old_root.McoPatientTransformer._
    val result: DataFrame = input.getDeathDates.select(col("patientID"), col("deathDate"))

    // Then
    import RichDataFrames._
    assert(result === expected)
  }

  it should "choose minimum death date if a patient has more than one death dates" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: DataFrame = Seq(
      ("Patient_01", 9, 2, 1985),
      ("Patient_01", 9, 4, 1980)
    ).toDF("patientID", "SOR_MOD", "SOR_MOI", "SOR_ANN")

    val expected: DataFrame = Seq(
      ("Patient_01", Timestamp.valueOf("1980-04-01 00:00:00"))
    ).toDF("patientID", "deathDate")

    // When
    import fr.polytechnique.cmap.cnam.etl.old_root.McoPatientTransformer._
    val result: DataFrame = input.getDeathDates.select(col("patientID"), col("deathDate"))

    // Then
    import RichDataFrames._
    assert(result === expected)
  }

  "transform" should "return correct Dataset" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val mco: DataFrame = Seq(
      ("Patient_01", 1, 2, 1985),
      ("Patient_02", 9, 3, 1986),
      ("Patient_03", 9, 4, 1980),
      ("Patient_03", 9, 4, 1984),
      ("Patient_04", 3, 5, 1995)
    ).toDF("NUM_ENQ", "MCO_B.SOR_MOD", "SOR_MOI", "SOR_ANN")
    val sources = new Sources(pmsiMco = Some(mco))


    val expected: DataFrame = Seq(
      ("Patient_02", Timestamp.valueOf("1986-03-01 00:00:00")),
      ("Patient_03", Timestamp.valueOf("1980-04-01 00:00:00"))
    ).toDF("patientID", "deathDate")

    // When
    val result = McoPatientTransformer.transform(sources)


    // Then
    import RichDataFrames._
    assert(result.toDF === expected)
  }
}
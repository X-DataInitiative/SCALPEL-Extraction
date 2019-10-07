// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.patients

import java.sql.Timestamp
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.SharedContext

class McoPatientsSuite extends SharedContext {

  import fr.polytechnique.cmap.cnam.etl.extractors.patients.McoPatients.McoPatientsDataFrame

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
    val result: DataFrame = input.getDeathDates(9).select(col("patientID"), col("deathDate"))

    // Then
    assertDFs(result, expected)
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
    val result: DataFrame = input.getDeathDates(9).select(col("patientID"), col("deathDate"))

    // Then
    assertDFs(result, expected)
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
    ).toDF("NUM_ENQ", "MCO_B__SOR_MOD", "SOR_MOI", "SOR_ANN")

    val expected: DataFrame = Seq(
      ("Patient_02", Timestamp.valueOf("1986-03-01 00:00:00")),
      ("Patient_03", Timestamp.valueOf("1980-04-01 00:00:00"))
    ).toDF("patientID", "deathDate")

    // When
    val result = McoPatients.extract(mco)

    // Then
    assertDFs(result.toDF, expected)
  }
}
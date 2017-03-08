package fr.polytechnique.cmap.cnam.etl.old_root

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.RichDataFrames

/**
  * @author Daniel de Paula
  */
class DcirPatientTransformerSuite extends SharedContext {


  "findBirthYears" should "return a DataFrame with the birth year for each patient" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: DataFrame = Seq(
      ("Patient_01", "1975"),
      ("Patient_01", "1975"),
      ("Patient_01", "000000"),
      ("Patient_01", "999999"),
      ("Patient_01", "2075"),
      ("Patient_01", "1975"),
      ("Patient_02", "1959"),
      ("Patient_02", "1959"),
      ("Patient_02", "9999"),
      ("Patient_02", "9999")
    ).toDF("patientID", "birthYear")
    val expectedResult: DataFrame = Seq(
      ("Patient_01", "1975"),
      ("Patient_02", "1959")
    ).toDF("patientID", "birthYear")

    // When
    import fr.polytechnique.cmap.cnam.etl.old_root.DcirPatientTransformer._
    val result = input.findBirthYears

    // Then
    import RichDataFrames._
    assert(result === expectedResult)
  }

  "groupByIdAndAge" should "return a DataFrame with data aggregated by patient ID and age" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val givenDf: DataFrame = sqlContext.read.parquet("src/test/resources/expected/DCIR.parquet")
    val input: DataFrame = Seq(
      ("Patient_01", 31, 2, Date.valueOf("2006-01-15"), None),
      ("Patient_01", 31, 2, Date.valueOf("2006-01-15"), None),
      ("Patient_01", 31, 2, Date.valueOf("2006-01-30"), None),
      ("Patient_02", 47, 1, Date.valueOf("2006-01-05"), Some(Date.valueOf("2009-03-13"))),
      ("Patient_02", 47, 1, Date.valueOf("2006-01-15"), Some(Date.valueOf("2009-03-13"))),
      ("Patient_02", 47, 1, Date.valueOf("2006-01-30"), Some(Date.valueOf("2009-03-13"))),
      ("Patient_02", 47, 1, Date.valueOf("2006-01-30"), Some(Date.valueOf("2009-03-13")))
    ).toDF("patientID", "age", "gender", "eventDate", "deathDate")

    val expected: DataFrame = Seq(
      ("Patient_01", 31, 3L, 6L, Date.valueOf("2006-01-15"), Date.valueOf("2006-01-30"),
        None),
      ("Patient_02", 47, 4L, 4L, Date.valueOf("2006-01-05"), Date.valueOf("2006-01-30"),
        Some(Date.valueOf("2009-03-13")))
    ).toDF("patientID", "age", "genderCount", "genderSum", "minEventDate", "maxEventDate",
      "deathDate")

    // When
    import fr.polytechnique.cmap.cnam.etl.old_root.DcirPatientTransformer._
    val result = input.groupByIdAndAge

    // Then
    import RichDataFrames._
    result.show(false)
    expected.show(false)
    assert(result === expected)
  }

  "estimateFields" should "return a Dataset[Patient] from a DataFrame with aggregated data" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: DataFrame = Seq(
      ("Patient_01", 31, 3, 6, "1975", Date.valueOf("2006-01-15"),
        Date.valueOf("2006-01-30"), None),
      ("Patient_02", 47, 4, 4, "1959", Date.valueOf("2006-01-05"),
        Date.valueOf("2006-01-30"), Some(Date.valueOf("2009-03-13")))
    ).toDF("patientID", "age", "genderCount", "genderSum", "birthYear", "minEventDate",
      "maxEventDate", "deathDate")

    val expected: DataFrame = Seq(
      Patient(
        patientID = "Patient_01",
        gender = 2,
        birthDate = Timestamp.valueOf("1975-01-01 00:00:00"),
        deathDate = None
      ),
      Patient(
        patientID = "Patient_02",
        gender = 1,
        birthDate = Timestamp.valueOf("1959-01-01 00:00:00"),
        deathDate = Some(Timestamp.valueOf("2009-03-13 00:00:00"))
      )
    ).toDF

    // When
    import fr.polytechnique.cmap.cnam.etl.old_root.DcirPatientTransformer._
    val result = input.estimateFields.toDF

    // Then
    import RichDataFrames._
    assert(result === expected)
  }

  "transform" should "return the correct data in a Dataset[Patient] for a known input" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: DataFrame = sqlCtx.read.parquet("src/test/resources/expected/DCIR.parquet")
    val sources = new Sources(dcir = Some(input))
    val expected: DataFrame = Seq(
      Patient(
        patientID = "Patient_01",
        gender = 2,
        birthDate = Timestamp.valueOf("1975-01-01 00:00:00"),
        deathDate = None
      ),
      Patient(
        patientID = "Patient_02",
        gender = 1,
        birthDate = Timestamp.valueOf("1959-01-01 00:00:00"),
        deathDate = Some(Timestamp.valueOf("2009-03-13 00:00:00"))
      )
    ).toDF

    // When
    val result = DcirPatientTransformer.transform(sources).toDF

    // Then
    result.show()
    expected.show()
    import RichDataFrames._
    assert(result === expected)
  }
}

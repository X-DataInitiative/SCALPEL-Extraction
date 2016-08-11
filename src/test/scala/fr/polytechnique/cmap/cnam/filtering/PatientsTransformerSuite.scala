package fr.polytechnique.cmap.cnam.filtering

import java.sql.Date
import java.sql.Timestamp
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames

/**
  * @author Daniel de Paula
  */
class PatientsTransformerSuite extends SharedContext {

  "selectFromDCIR" should "return a DataFrame with a subset of the original columns" in {
    // Given
    val givenDf: DataFrame = sqlContext.read.parquet("src/test/resources/expected/DCIR.parquet")
    val expectedSchema = StructType(
      StructField("patientID", StringType, true) ::
      StructField("gender", IntegerType, true) ::
      StructField("age", IntegerType, true) ::
      StructField("birthYear", StringType, true) ::
      StructField("eventDate", DateType, true) ::
      StructField("eventMonth", IntegerType, true) ::
      StructField("eventYear", IntegerType, true) ::
      StructField("deathDate", DateType, true) :: Nil
    )

    // When
    import PatientsTransformer._
    val resultSchema = givenDf.selectFromDCIR.schema

    // Then
    assert(resultSchema == expectedSchema)
  }

  it should "return a DataFrame without out-of-bounds values" in {
    // Given
    val givenDf: DataFrame = sqlContext.read.parquet("src/test/resources/expected/DCIR.parquet")
    val expectedCount = 0L

    // When
    import PatientsTransformer._
    val data = givenDf.selectFromDCIR
    val resultCount = data.filter(
      !col("age").between(data.MinAge, data.MaxAge) ||
      !year(col("deathDate")).between(data.MinYear, data.MaxYear) ||
      !col("gender").between(data.MinGender, data.MaxGender)
    ).count

    // Then
    assert(resultCount == expectedCount)
  }

  "findBirthYears" should "return a DataFrame with the birth year for each patient" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val givenDf: DataFrame = Seq(
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
    import PatientsTransformer._
    val result = givenDf.findBirthYears

    // Then
    import RichDataFrames._
    assert(result === expectedResult)
  }

  "groupByIdAndAge" should "return a DataFrame with data aggregated by patient ID and age" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val givenDf: DataFrame = sqlContext.read.parquet("src/test/resources/expected/DCIR.parquet")
    val expectedResult: DataFrame = Seq(
      ("Patient_01", 31, 3, 6, Date.valueOf("2006-01-15"), Date.valueOf("2006-01-30"), None),
      ("Patient_02", 47, 4, 4,
        Date.valueOf("2006-01-05"), Date.valueOf("2006-01-30"), Some(Date.valueOf("2009-03-13")))
    ).toDF("patientID", "age", "genderCount", "genderSum", "minEventDate", "maxEventDate", "deathDate")

    // When
    import PatientsTransformer._
    val result = givenDf
      .selectFromDCIR
      .groupByIdAndAge

    // Then
    import RichDataFrames._
    assert(result === expectedResult)
  }

  "estimateFields" should "return a Dataset[Patient] from a DataFrame with aggregated data" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val givenDf: DataFrame = Seq(
      ("Patient_01", 31, 3, 6, "1975", Date.valueOf("2006-01-15"), Date.valueOf("2006-01-30"), None),
      ("Patient_02", 47, 4, 4, "1959",
        Date.valueOf("2006-01-05"), Date.valueOf("2006-01-30"), Some(Date.valueOf("2009-03-13")))
    ).toDF("patientID", "age", "genderCount", "genderSum", "birthYear", "minEventDate",
      "maxEventDate", "deathDate")

    val expectedResult: DataFrame = Seq(
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
    import PatientsTransformer._
    val result = givenDf.estimateFields.toDF

    // Then
    import RichDataFrames._
    assert(result === expectedResult)
  }

  "transform" should "return the correct data in a Dataset[Patient] for a known input" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val givenDf: DataFrame = sqlCtx.read.parquet("src/test/resources/expected/DCIR.parquet")
    val sources = new Sources(dcir = Some(givenDf))

    // When
    val result = PatientsTransformer.transform(sources).toDF
    val expectedResult: DataFrame = Seq(
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

    // Then
    import RichDataFrames._
    assert(result === expectedResult)
  }
}

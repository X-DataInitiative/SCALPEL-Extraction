package fr.polytechnique.cmap.cnam.filtering

import java.sql.Date
import java.sql.Timestamp
import org.apache.spark.sql.{Column, DataFrame}
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
      ("Patient_01", 31, 3L, 6L, Date.valueOf("2006-01-15"), Date.valueOf("2006-01-30"), None),
      ("Patient_02", 47, 4L, 4L,
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
      ("Patient_01", 2, Timestamp.valueOf("1975-01-01 00:00:00"), None),
      ("Patient_02", 1,Timestamp.valueOf("1959-01-01 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00")))
    ).toDF("patientID", "dcirGender", "dcirBirthDate", "dcirMinDeathDate")

    // When
    import PatientsTransformer._
    val result = givenDf.estimateDCIRFields.toDF

    // Then
    import RichDataFrames._
    assert(result === expectedResult)
  }

  "getIrBenBirthDates" should "collect birth dates correctly from IR_BEN_R" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val irBen: DataFrame = Seq(
      ("Patient_01", 1, 1, 1975),
      ("Patient_02", 1, 2, 1976),
      ("Patient_03", 2, 3, 1977),
      ("Patient_04", 2, 4, 1895)
    ).toDF("NUM_ENQ", "BEN_SEX_COD", "BEN_NAI_MOI", "BEN_NAI_ANN")

    val expectedResultBirthDate: DataFrame = Seq(
      ("Patient_01", 1, Timestamp.valueOf("1975-01-01 00:00:00")),
      ("Patient_02", 1, Timestamp.valueOf("1976-02-01 00:00:00")),
      ("Patient_03", 2, Timestamp.valueOf("1977-03-01 00:00:00"))
    ).toDF("patientID", "irBenGender", "irBenBirthDate")

    // When
    import PatientsTransformer._
    val irBenBirthDates = irBen.getIrBenBirthDatesAndGender

    // Then
    import RichDataFrames._
    assert(irBenBirthDates === expectedResultBirthDate)
  }

  "getIrBenDeathDates" should "collect death dates correctly from IR_BEN_R" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val irBen: DataFrame = Seq(
      ("Patient_01", None),
      ("Patient_02", Some(Timestamp.valueOf("2009-03-13 00:00:00"))),
      ("Patient_03", Some(Timestamp.valueOf("1976-03-13 00:00:00"))),
      ("Patient_04", Some(Timestamp.valueOf("2020-03-13 00:00:00")))
    ).toDF("NUM_ENQ", "BEN_DCD_DTE")

    val expectedResultDeathDate: DataFrame = Seq(
      ("Patient_04", Timestamp.valueOf("2020-03-13 00:00:00")),
      ("Patient_03", Timestamp.valueOf("1976-03-13 00:00:00")),
      ("Patient_02", Timestamp.valueOf("2009-03-13 00:00:00"))
    ).toDF("patientID", "irBenDeathDate")

    // When
    import PatientsTransformer._
    val irBenDeathDates = irBen.getIrBenDeathDates

    // Then
    import RichDataFrames._
    assert(irBenDeathDates === expectedResultDeathDate)
  }

  "getMcoDeathDates" should "collect death dates correctly from flat MCO" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val pmsiMCO: DataFrame = Seq(
      ("Patient_01", 1, 2, 1985),
      ("Patient_02", 9, 3, 1986),
      ("Patient_03", 9, 4, 1980),
      ("Patient_04", 3, 5, 1995)
    ).toDF("patientID", "SOR_MOD", "SOR_MOI", "SOR_ANN")

    val expectedResult: DataFrame = Seq(
      ("Patient_02", Timestamp.valueOf("1986-03-01 00:00:00")),
      ("Patient_03", Timestamp.valueOf("1980-04-01 00:00:00"))
    ).toDF("patientID", "mcoDeathDate")

    // When
    import PatientsTransformer._
    val result: DataFrame = pmsiMCO.getMcoDeathDates

    // Then
    import RichDataFrames._
    assert(result === expectedResult)
  }

  it should("choose minimum death date if a patient has more than one death dates") in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val pmsiMCO: DataFrame = Seq(
      ("Patient_01", 1, 2, 1985),
      ("Patient_02", 9, 3, 1986),
      ("Patient_03", 9, 4, 1980),
      ("Patient_03", 9, 3, 1980),
      ("Patient_04", 3, 5, 1995)
    ).toDF("patientID", "SOR_MOD", "SOR_MOI", "SOR_ANN")

    val expectedResult: DataFrame = Seq(
      ("Patient_02", Timestamp.valueOf("1986-03-01 00:00:00")),
      ("Patient_03", Timestamp.valueOf("1980-03-01 00:00:00"))
    ).toDF("patientID", "mcoDeathDate")

    // When
    import PatientsTransformer._
    val result: DataFrame = pmsiMCO.getMcoDeathDates

    // Then
    import RichDataFrames._
    assert(result === expectedResult)
  }

  "transform" should "return the correct data in a Dataset[Patient] for a known input" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val dcirDf: DataFrame = Seq(
      ("Patient_01", 2, 31, 1975, Some(Timestamp.valueOf("2006-01-15 00:00:00")), None),
      ("Patient_01", 2, 31, 1975, Some(Timestamp.valueOf("2006-01-30 00:00:00")), None),
      ("Patient_02", 1, 47, 1959, Some(Timestamp.valueOf("2006-01-15 00:00:00")), Some(Timestamp.valueOf("2009-03-13 00:00:00"))),
      ("Patient_02", 1, 47, 1959, Some(Timestamp.valueOf("2006-01-30 00:00:00")), Some(Timestamp.valueOf("2009-03-13 00:00:00"))),
      ("Patient_03", 1, 47, 1959, Some(Timestamp.valueOf("2006-01-30 00:00:00")), None),
      ("Patient_04", 1, 51, 1969, Some(Timestamp.valueOf("2006-01-05 00:00:00")), Some(Timestamp.valueOf("2009-03-13 00:00:00"))),
      ("Patient_04", 1, 51, 1969, Some(Timestamp.valueOf("2006-02-05 00:00:00")), None),
      ("Patient_04", 2, 51, 1969, Some(Timestamp.valueOf("2006-03-05 00:00:00")), None)
    ).toDF("NUM_ENQ", "BEN_SEX_COD", "BEN_AMA_COD", "BEN_NAI_ANN", "EXE_SOI_DTD", "BEN_DCD_DTE")

    val mcoDf: DataFrame = Seq(
      ("Patient_01", 1, 2, 1985),
      ("Patient_02", 9, 3, 1986),
      ("Patient_03", 9, 4, 1980),
      ("Patient_04", 3, 5, 1995)
    ).toDF("NUM_ENQ", "MCO_B.SOR_MOD", "SOR_MOI", "SOR_ANN")

    val irBenDf: DataFrame = Seq(
      ("Patient_01", 1, 1, 1975, None),
      ("Patient_02", 1, 2, 1976, Some(Timestamp.valueOf("2009-03-13 00:00:00"))),
      ("Patient_03", 2, 3, 1977, Some(Timestamp.valueOf("1976-03-13 00:00:00"))),
      ("Patient_04", 2, 4, 1895, Some(Timestamp.valueOf("2020-03-13 00:00:00")))
    ).toDF("NUM_ENQ", "BEN_SEX_COD", "BEN_NAI_MOI", "BEN_NAI_ANN", "BEN_DCD_DTE")

    val sources = new Sources(dcir = Some(dcirDf), pmsiMco = Some(mcoDf), irBen = Some(irBenDf))

    // When
    val result = PatientsTransformer.transform(sources).toDF
    val expectedResult: DataFrame = Seq(
      ("Patient_01", 1, Timestamp.valueOf("1975-01-01 00:00:00"), None),
      ("Patient_02", 1, Timestamp.valueOf("1976-02-01 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00"))),
      ("Patient_03", 2, Timestamp.valueOf("1977-03-01 00:00:00"), Some(Timestamp.valueOf("1980-04-01 00:00:00"))),
      ("Patient_04", 1, Timestamp.valueOf("1969-02-01 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00")))
    ).toDF("patientID", "gender", "birthDate", "deathDate")

    // Then
    import RichDataFrames._
    assert(result === expectedResult)
  }

}

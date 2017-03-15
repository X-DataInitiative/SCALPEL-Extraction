package fr.polytechnique.cmap.cnam.etl.patients

import java.sql.Timestamp
import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.etl.sources.Sources

class IrBenPatientsSuite extends SharedContext {

  import fr.polytechnique.cmap.cnam.etl.patients.IrBenPatients.IrBenPatientsDataFrame

  "getBirthDates" should "collect birth dates correctly from IR_BEN_R" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val irBen: DataFrame = Seq(
      ("Patient_01", 1, 1975),
      ("Patient_02", 2, 1976),
      ("Patient_03", 3, 1977),
      ("Patient_04", 4, 1895)
    ).toDF("patientID", "BEN_NAI_MOI", "BEN_NAI_ANN")

    val expected: DataFrame = Seq(
      ("Patient_01", Timestamp.valueOf("1975-01-01 00:00:00")),
      ("Patient_02", Timestamp.valueOf("1976-02-01 00:00:00")),
      ("Patient_03", Timestamp.valueOf("1977-03-01 00:00:00"))
    ).toDF("patientID", "birthDate")

    // When
    val result = irBen.getBirthDate()

    // Then
    assertDFs(result, expected)
  }

  it should "throw an exception in case of conflicting birth dates" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val irBen: DataFrame = Seq(
      ("Patient_01", 1, 1975),
      ("Patient_01", 2, 1976)
    ).toDF("patientID", "BEN_NAI_MOI", "BEN_NAI_ANN")

    // Then
    intercept[Exception] {
      irBen.getBirthDate()
    }
  }

  "getGender" should "return correct gender" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: DataFrame = Seq(
      ("Patient_01", 1),
      ("Patient_02", 2),
      ("Patient_02", 2)
    ).toDF("patientID", "BEN_SEX_COD")

    val expected: DataFrame = Seq(
      ("Patient_01", 1),
      ("Patient_02", 2)
    ).toDF("patientID", "gender")

    // When
    val result = input.getGender

    // Then
    assertDFs(result, expected)
  }

  it should "throw an exception in case of conflicting sex code" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: DataFrame = Seq(
      ("Patient_01", 1),
      ("Patient_01", 2)
    ).toDF("patientID", "BEN_SEX_COD")

    // Then
    intercept[Exception] {
      input.getGender
    }
  }

  "getDeathDate" should "collect death dates correctly from IR_BEN_R" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val irBen: DataFrame = Seq(
      ("Patient_01", None),
      ("Patient_02", Some(Timestamp.valueOf("2009-03-13 00:00:00"))),
      ("Patient_03", Some(Timestamp.valueOf("1976-03-13 00:00:00"))),
      ("Patient_04", Some(Timestamp.valueOf("2020-03-13 00:00:00")))
    ).toDF("patientID", "BEN_DCD_DTE")

    val expected: DataFrame = Seq(
      ("Patient_04", Timestamp.valueOf("2020-03-13 00:00:00")),
      ("Patient_03", Timestamp.valueOf("1976-03-13 00:00:00")),
      ("Patient_02", Timestamp.valueOf("2009-03-13 00:00:00"))
    ).toDF("patientID", "deathDate")

    // When
    val result = irBen.getDeathDate

    // Then
    assertDFs(result, expected)
  }

  "transform" should "return correct result" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val config = ExtractionConfig.init()
    val irBen: DataFrame = Seq(
      ("Patient_01", 1, 1, 1975, Timestamp.valueOf("2009-03-13 00:00:00")),
      ("Patient_02", 2, 3, 1977, null.asInstanceOf[Timestamp]),
      ("Patient_02", 2, 4, 1895, null.asInstanceOf[Timestamp])
    ).toDF("NUM_ENQ", "BEN_SEX_COD", "BEN_NAI_MOI", "BEN_NAI_ANN", "BEN_DCD_DTE")

    val expected: DataFrame = Seq(
      ("Patient_01", 1, Timestamp.valueOf("1975-01-01 00:00:00"), Timestamp.valueOf("2009-03-13 00:00:00")),
      ("Patient_02", 2, Timestamp.valueOf("1977-03-01 00:00:00"), null.asInstanceOf[Timestamp])
    ).toDF("patientID", "gender", "birthDate", "deathDate")

    val input = new Sources(irBen = Some(irBen))

    // When
    val result = IrBenPatients.extract(config, irBen)

    // Then
    assertDFs(result.toDF, expected)
 }

  it should "deal with actual data" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val config = ExtractionConfig.init()
    val irBen = sqlCtx.read.load("src/test/resources/expected/IR_BEN_R.parquet")
    val input = new Sources(irBen = Some(irBen))


    val expected: DataFrame = Seq(
      ("Patient_01", 2, Timestamp.valueOf("1975-01-01 00:00:00"), null.asInstanceOf[Timestamp]),
      ("Patient_02", 1, Timestamp.valueOf("1959-10-01 00:00:00"), Timestamp.valueOf("2008-01-25 00:00:00"))
    ).toDF("patientID", "gender", "birthDate", "deathDate")

    // When
    val result = IrBenPatients.extract(config, irBen)

    // Then
    assertDFs(result.toDF, expected)



  }
}

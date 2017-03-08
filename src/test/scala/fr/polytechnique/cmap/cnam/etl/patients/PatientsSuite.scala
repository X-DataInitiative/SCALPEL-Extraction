package fr.polytechnique.cmap.cnam.etl.patients

import org.apache.spark.sql.{Column, DataFrame}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.RichDataFrames
import fr.polytechnique.cmap.cnam.util.functions._

class PatientsSuite extends SharedContext {

  "isDeathDateValid" should "remove absurd deathDate" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val df: DataFrame = Seq(
      (makeTS(1989, 3, 13), makeTS(2029, 3, 13)),
      (makeTS(1989, 3, 13), makeTS(2009, 3, 13)),
      (makeTS(1989, 3, 13), makeTS(1979, 3, 13))
    ).toDF("birthDate", "deathDate")

    val deathDates: Column = df("deathDate")
    val birthDates: Column = df("birthDate")

    val expected = 1

    // When
    val result = df
      .filter(Patients.validateDeathDate(deathDates, birthDates, 2020) === true)
      .count

    // Then
    assert(result == expected)
  }

  "transform" should "return the correct data in a Dataset[Patient] for a known input" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val config = ExtractionConfig.init()
    val dcirDf: DataFrame = Seq(
      ("Patient_01", 2, 31, 1945, Some(makeTS(2006, 1, 15)), None),
      ("Patient_01", 2, 31, 1945, Some(makeTS(2006, 1, 30)), None),
      ("Patient_02", 1, 47, 1959, Some(makeTS(2006, 1, 15)), Some(makeTS(2009, 3, 13))),
      ("Patient_02", 1, 47, 1959, Some(makeTS(2006, 1, 30)), Some(makeTS(2009, 3, 13))),
      ("Patient_03", 1, 47, 1959, Some(makeTS(2006, 1, 30)), None),
      ("Patient_04", 1, 51, 1966, Some(makeTS(2006, 1,  5)), Some(makeTS(2009, 3, 13))),
      ("Patient_04", 1, 51, 1966, Some(makeTS(2006, 2,  5)), None),
      ("Patient_04", 2, 51, 1966, Some(makeTS(2006, 3,  5)), None)
    ).toDF("NUM_ENQ", "BEN_SEX_COD", "BEN_AMA_COD", "BEN_NAI_ANN", "EXE_SOI_DTD", "BEN_DCD_DTE")

    val mcoDf: DataFrame = Seq(
      ("Patient_01", 1, 2, 1985),
      ("Patient_02", 9, 3, 1986),
      ("Patient_03", 9, 4, 1980),
      ("Patient_04", 3, 5, 1995)
    ).toDF("NUM_ENQ", "MCO_B.SOR_MOD", "SOR_MOI", "SOR_ANN")

    val irBenDf: DataFrame = Seq(
      ("Patient_01", 1, 1, 1945, None),
      ("Patient_02", 1, 2, 1956, Some(makeTS(2009, 3, 13))),
      ("Patient_03", 2, 3, 1937, Some(makeTS(1936, 3, 13))),
      ("Patient_04", 2, 4, 1895, Some(makeTS(2020, 3, 13)))
    ).toDF("NUM_ENQ", "BEN_SEX_COD", "BEN_NAI_MOI", "BEN_NAI_ANN", "BEN_DCD_DTE")

    val sources = new Sources(dcir = Some(dcirDf), pmsiMco = Some(mcoDf), irBen = Some(irBenDf))

    // When
    val result = Patients.extract(config, sources).toDF
    val expected: DataFrame = Seq(
      ("Patient_01", 1, makeTS(1945, 1, 1), None),
      ("Patient_02", 1, makeTS(1956, 2, 1), Some(makeTS(2009, 3, 13))),
      ("Patient_03", 2, makeTS(1937, 3, 1), Some(makeTS(1980, 4,  1))),
      ("Patient_04", 2, makeTS(1966, 2, 1), Some(makeTS(2009, 3, 13)))
    ).toDF("patientID", "gender", "birthDate", "deathDate")

    // Then
    import RichDataFrames._
    result.show(false)
    result.printSchema()
    expected.show(false)
    expected.printSchema()
    assert(result === expected)
  }

}

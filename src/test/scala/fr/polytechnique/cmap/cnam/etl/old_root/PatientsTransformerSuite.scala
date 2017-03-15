package fr.polytechnique.cmap.cnam.etl.old_root

import java.sql.Timestamp
import org.apache.spark.sql.{Column, DataFrame}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.sources.Sources

class PatientsTransformerSuite extends SharedContext {

  "isDeathDateValid" should "remove absurd deathDate" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val df: DataFrame = Seq(
      (Timestamp.valueOf("1989-03-13 00:00:00"),Timestamp.valueOf("2029-03-13 00:00:00")),
      (Timestamp.valueOf("1989-03-13 00:00:00"),Timestamp.valueOf("2009-03-13 00:00:00")),
      (Timestamp.valueOf("1989-03-13 00:00:00"),Timestamp.valueOf("1979-03-13 00:00:00"))
    ).toDF("birthDate", "deathDate")

    val deathDates: Column = df("deathDate")
    val birthDates: Column = df("birthDate")

    val expected = 1

    // When
    import fr.polytechnique.cmap.cnam.etl.old_root.PatientsTransformer._
    val result =
      df
      .filter(isDeathDateValid(deathDates, birthDates) === true)
      .count

    // Then
    assert(result == expected)
  }

  "transform" should "return the correct data in a Dataset[Patient] for a known input" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val dcirDf: DataFrame = Seq(
      ("Patient_01", 2, 31, 1945, Some(Timestamp.valueOf("2006-01-15 00:00:00")), None),
      ("Patient_01", 2, 31, 1945, Some(Timestamp.valueOf("2006-01-30 00:00:00")), None),
      ("Patient_02", 1, 47, 1959, Some(Timestamp.valueOf("2006-01-15 00:00:00")), Some(Timestamp.valueOf("2009-03-13 00:00:00"))),
      ("Patient_02", 1, 47, 1959, Some(Timestamp.valueOf("2006-01-30 00:00:00")), Some(Timestamp.valueOf("2009-03-13 00:00:00"))),
      ("Patient_03", 1, 47, 1959, Some(Timestamp.valueOf("2006-01-30 00:00:00")), None),
      ("Patient_04", 1, 51, 1966, Some(Timestamp.valueOf("2006-01-05 00:00:00")), Some(Timestamp.valueOf("2009-03-13 00:00:00"))),
      ("Patient_04", 1, 51, 1966, Some(Timestamp.valueOf("2006-02-05 00:00:00")), None),
      ("Patient_04", 2, 51, 1966, Some(Timestamp.valueOf("2006-03-05 00:00:00")), None)
    ).toDF("NUM_ENQ", "BEN_SEX_COD", "BEN_AMA_COD", "BEN_NAI_ANN", "EXE_SOI_DTD", "BEN_DCD_DTE")

    val mcoDf: DataFrame = Seq(
      ("Patient_01", 1, 2, 1985),
      ("Patient_02", 9, 3, 1986),
      ("Patient_03", 9, 4, 1980),
      ("Patient_04", 3, 5, 1995)
    ).toDF("NUM_ENQ", "MCO_B.SOR_MOD", "SOR_MOI", "SOR_ANN")

    val irBenDf: DataFrame = Seq(
      ("Patient_01", 1, 1, 1945, None),
      ("Patient_02", 1, 2, 1956, Some(Timestamp.valueOf("2009-03-13 00:00:00"))),
      ("Patient_03", 2, 3, 1937, Some(Timestamp.valueOf("1936-03-13 00:00:00"))),
      ("Patient_04", 2, 4, 1895, Some(Timestamp.valueOf("2020-03-13 00:00:00")))
    ).toDF("NUM_ENQ", "BEN_SEX_COD", "BEN_NAI_MOI", "BEN_NAI_ANN", "BEN_DCD_DTE")

    val sources = new Sources(dcir = Some(dcirDf), pmsiMco = Some(mcoDf), irBen = Some(irBenDf))

    // When
    val result = PatientsTransformer.transform(sources).toDF
    val expected: DataFrame = Seq(
      ("Patient_01", 1, Timestamp.valueOf("1945-01-01 00:00:00"), None),
      ("Patient_02", 1, Timestamp.valueOf("1956-02-01 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00"))),
      ("Patient_03", 2, Timestamp.valueOf("1937-03-01 00:00:00"), Some(Timestamp.valueOf("1980-04-01 00:00:00"))),
      ("Patient_04", 2, Timestamp.valueOf("1966-02-01 00:00:00"), Some(Timestamp.valueOf("2009-03-13 00:00:00")))
    ).toDF("patientID", "gender", "birthDate", "deathDate")

    // Then
    assertDFs(result, expected)
  }

}

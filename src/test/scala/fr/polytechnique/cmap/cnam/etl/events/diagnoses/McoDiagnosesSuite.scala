package fr.polytechnique.cmap.cnam.etl.events.diagnoses

import java.sql.Timestamp
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class McoDiagnosesSuite extends SharedContext {

  lazy val fakeMcoData = {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    Seq(
      ("HasCancer1", Some("C669"), Some("C672"), Some("C643"), Some(12), Some(2011), 11,
        Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12))),
      ("HasCancer1", Some("C679"), Some("C691"), Some("C643"),Some(12), Some(2011), 11,
        Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12))),
      ("HasCancer2", Some("C669"), Some("C672"), Some("C643"), Some(12), Some(2011), 11,
        None, Some(makeTS(2011, 12, 12))),
      ("HasCancer3", Some("C669"), Some("C672"), Some("C643"), Some(12), Some(2011), 11,
        None, None),
      ("HasCancer4", Some("C669"), Some("C672"), Some("C643"), None, None, 11,
        None, Some(makeTS(2011, 12, 12))),
      ("HasCancer5", Some("C679"), Some("B672"), Some("C673"), Some(1), Some(2010), 31,
        Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12))),
      ("MustBeDropped1", None, None, None, Some(1), Some(2010), 31,
        Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12))),
      ("MustBeDropped2", None, Some("7"), None, Some(1), Some(2010), 31,
        Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12)))
    ).toDF("NUM_ENQ", "MCO_D.ASS_DGN", "MCO_B.DGN_PAL", "MCO_B.DGN_REL", "MCO_B.SOR_MOI",
      "MCO_B.SOR_ANN", "MCO_B.SEJ_NBJ", "ENT_DAT", "SOR_DAT")
  }

  "estimateStayStartTime" should "estimate a stay starting date using MCO available data" in {

    // Given
    val input = fakeMcoData
    val expected: List[Timestamp] = (List.fill(2)(makeTS(2011, 12, 1)) :+ makeTS(2011, 11, 20)) :::
      List.fill(4)(makeTS(2011, 12, 1))


    // When
    import McoDiagnoses.McoDataFrame
    val output: List[Timestamp] = input.select(McoDiagnoses.inputColumns: _*)
      .estimateStayStartTime
      .select("eventDate")
      .take(expected.length)
      .map(_.getAs[Timestamp](0))
      .toList

    // Then
    assert(output.map(_.getTime()).sorted == expected.map(_.getTime()).sorted)
  }

  it should "consider null values of MCO_B.SEJ_NBJ as 0" in {
    // Given
    val fakeMCOWithNullSEJ = {

      val sqlCtx = sqlContext
      import sqlCtx.implicits._

      Seq(
        ("HasCancer1", Some("C669"), Some("C672"), Some("C643"), Some(12), Some(2011), Some(11),
          Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12))),
        ("HasCancer2", Some("C669"), Some("C672"), Some("C643"), Some(12), Some(2011), None,
          None, Some(makeTS(2011, 12, 12))),
        ("HasCancer3", Some("C669"), Some("C672"), Some("C643"), Some(12), Some(2011), Some(11),
          None, None),
        ("HasCancer3.1", Some("C669"), Some("C672"), Some("C643"), Some(12), Some(2011), None,
          None, None)
      ).toDF(fakeMcoData.columns: _*)
    }

    val input = fakeMCOWithNullSEJ
    val expected: List[Timestamp] = List(makeTS(2011, 12, 1), makeTS(2011, 12, 12),
      makeTS(2011, 11, 20), makeTS(2011, 12, 1))

    // When
    import McoDiagnoses.McoDataFrame
    val output: List[Timestamp] = input.select(McoDiagnoses.inputColumns: _*)
      .estimateStayStartTime
      .select("eventDate")
      .take(expected.length)
      .map(_.getAs[Timestamp](0))
      .toList

    assert(output.map(_.getTime()).sorted == expected.map(_.getTime()).sorted)
  }

  "findCodesInColumn" should "return a column with the found codes" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = fakeMcoData
    val codes = List("C67", "C69")
    val column = col("`MCO_B.DGN_PAL`")

    val expected: DataFrame = Seq(
      ("HasCancer1", Some("C67")),
      ("HasCancer1", Some("C69")),
      ("HasCancer2", Some("C67")),
      ("HasCancer3", Some("C67")),
      ("HasCancer4", Some("C67")),
      ("HasCancer5", None),
      ("MustBeDropped1", None),
      ("MustBeDropped2", None)
    ).toDF("NUM_ENQ", "codes")

    // When
    val result = input.select(col("NUM_ENQ"), McoDiagnoses.findCodesInColumn(column, codes).as("codes"))

    // Then
    assertDFs(result, expected)
  }

  "buildDiagnoses" should "convert a row in the format McoDiagnosesCodes to a list of diagnosis events" in {

    import McoDiagnoses.McoDiagnosesCodes

    // Given
    val input = Seq(
      McoDiagnosesCodes("Patient4", makeTS(2010, 4, 1), None, None, None),
      McoDiagnosesCodes("Patient5", makeTS(2010, 5, 1), Some("C55"), None, None),
      McoDiagnosesCodes("Patient6", makeTS(2010, 6, 1), Some("C56"), None, Some("C76")),
      McoDiagnosesCodes("Patient7", makeTS(2010, 7, 1), Some("C57"), Some("C67"), None),
      McoDiagnosesCodes("Patient8", makeTS(2010, 8, 1), Some("C58"), Some("C68"), Some("C78"))
    )

    val expected = Seq(
      MainDiagnosis("Patient5", "C55", makeTS(2010, 5, 1)),
      MainDiagnosis("Patient6", "C56", makeTS(2010, 6, 1)),
      AssociatedDiagnosis("Patient6", "C76", makeTS(2010, 6, 1)),
      MainDiagnosis("Patient7", "C57", makeTS(2010, 7, 1)),
      LinkedDiagnosis("Patient7", "C67", makeTS(2010, 7, 1)),
      MainDiagnosis("Patient8", "C58", makeTS(2010, 8, 1)),
      LinkedDiagnosis("Patient8", "C68", makeTS(2010, 8, 1)),
      AssociatedDiagnosis("Patient8", "C78", makeTS(2010, 8, 1))
    )

    // When
    val result = input.flatMap(McoDiagnoses.buildDiagnoses)

    // Then
    assert(result == expected)
  }

  "extract" should "extract diagnosis events from raw data" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val config = ExtractionConfig.init().copy(
      mainDiagnosisCodes = List("C67"),
      linkedDiagnosisCodes = List("C67"),
      associatedDiagnosisCodes = List("C67")
    )
    val expected = Seq(
      MainDiagnosis("HasCancer1", "C67", makeTS(2011, 12,  1)),
      AssociatedDiagnosis("HasCancer1", "C67", makeTS(2011, 12,  1)),
      MainDiagnosis("HasCancer2", "C67", makeTS(2011, 12,  1)),
      MainDiagnosis("HasCancer3", "C67", makeTS(2011, 11, 20)),
      MainDiagnosis("HasCancer4", "C67", makeTS(2011, 12,  1)),
      LinkedDiagnosis("HasCancer5", "C67", makeTS(2011, 12, 1)),
      AssociatedDiagnosis("HasCancer5", "C67", makeTS(2011, 12, 1))
    ).toDF

    // When
    val output = McoDiagnoses.extract(config, fakeMcoData)

    // Then
    assertDFs(expected.toDF, output.toDF)
  }
}


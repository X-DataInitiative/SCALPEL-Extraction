package fr.polytechnique.cmap.cnam.etl.events.diagnoses

import java.sql.Timestamp
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.etl.events.Event
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class McoDiagnosesSuite extends SharedContext {

  lazy val fakeMcoData = {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    Seq(
      ("HasCancer1", Some("C669"), Some("C668"), Some("C651"), Some("C672"), Some("C643"),
        Some(12), Some(2011), 11, Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12))
      ),
      ("HasCancer1", Some("C679"), Some("C669"), Some("C651"), Some("C691"), Some("C643"),
        Some(12), Some(2011), 11, Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12))
      ),
      ("HasCancer2", Some("C669"), Some("C668"), Some("C651"), Some("C672"), Some("C643"),
        Some(12), Some(2011), 11, None, Some(makeTS(2011, 12, 12))
      ),
      ("HasCancer3", Some("C669"), Some("C668"), Some("C651"), Some("C672"), Some("C643"),
        Some(12), Some(2011), 11, None, None
      ),
      ("HasCancer4", Some("C669"), Some("C668"), Some("C651"), Some("C672"), Some("C643"),
        None, None, 11, None, Some(makeTS(2011, 12, 12))
      ),
      ("HasCancer5", Some("C679"), Some("C678"), Some("C671"), Some("B672"), Some("C673"),
        Some(1), Some(2010), 31, Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12))
      ),
      ("MustBeDropped1", None, None, None, None, None,
        Some(1), Some(2010), 31, Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12))
      ),
      ("MustBeDropped2", None, None, Some("C556"), Some("7"), None,
        Some(1), Some(2010), 31, Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12))
      )
    ).toDF("NUM_ENQ", "MCO_D.ASS_DGN", "MCO_UM.DGN_PAL", "MCO_UM.DGN_REL", "MCO_B.DGN_PAL",
      "MCO_B.DGN_REL", "MCO_B.SOR_MOI", "MCO_B.SOR_ANN", "MCO_B.SEJ_NBJ", "ENT_DAT",
      "SOR_DAT")
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
      .select("event_date")
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
        ("HasCancer1", Some("C669"), Some("C668"), Some("C651"), Some("C672"), Some("C643"),
          Some(12), Some(2011), Some(11), Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12))
        ),
        ("HasCancer2", Some("C669"), Some("C668"), Some("C651"), Some("C672"), Some("C643"),
          Some(12), Some(2011), None, None, Some(makeTS(2011, 12, 12))
        ),
        ("HasCancer3", Some("C669"), Some("C668"), Some("C651"), Some("C672"), Some("C643"),
          Some(12), Some(2011), Some(11), None, None
        ),
        ("HasCancer3.1", Some("C669"), Some("C668"), Some("C651"), Some("C672"), Some("C643"),
          Some(12), Some(2011), None, None, None
        )
      ).toDF(fakeMcoData.columns: _*)
    }

    val input = fakeMCOWithNullSEJ
    val expected: List[Timestamp] = List(makeTS(2011, 12, 1), makeTS(2011, 12, 12),
      makeTS(2011, 11, 20), makeTS(2011, 12, 1))

    // When
    import McoDiagnoses.McoDataFrame
    val output: List[Timestamp] = input.select(McoDiagnoses.inputColumns: _*)
      .estimateStayStartTime
      .select("event_date")
      .take(expected.length)
      .map(_.getAs[Timestamp](0))
      .toList

    assert(output.map(_.getTime()).sorted == expected.map(_.getTime()).sorted)
  }

  "rowToDiagnoses" should "convert a row to a list of diagnosis events" in {

    object MockDiagnosis extends DiagnosisBuilder {
      val category = "mock_diagnosis"
    }

    // Given
    val colNames = List("code_col_1", "code_col_2")
    val colCodes = List("C67", "C68")

    val schema = StructType(
      StructField("patientID", StringType) ::
      StructField("code_col_1", StringType) ::
      StructField("code_col_2", StringType) ::
      StructField("event_date", TimestampType) :: Nil
    )

    val rows = List(
      Array[Any]("Patient01", "noiseC67noiseC68", "noiseC67noise", makeTS(2010, 1, 1)),
      Array[Any]("Patient01", "noiseC67noise", "noise", makeTS(2010, 2, 1)),
      Array[Any]("Patient01", "noise", "noiseC68noise", makeTS(2010, 3, 1)),
      Array[Any]("Patient01", "noise", "noise", makeTS(2010, 4, 1))
    ).map(new GenericRowWithSchema(_, schema))

    val expected = List[List[Event[Diagnosis]]](
      List(
        MockDiagnosis("Patient01", "C67", makeTS(2010, 1, 1)),
        MockDiagnosis("Patient01", "C68", makeTS(2010, 1, 1)),
        MockDiagnosis("Patient01", "C67", makeTS(2010, 1, 1))
      ).distinct,
      List(MockDiagnosis("Patient01", "C67", makeTS(2010, 2, 1))),
      List(MockDiagnosis("Patient01", "C68", makeTS(2010, 3, 1))),
      List[Event[Diagnosis]]()
    )

    // When
    val result = rows.map(McoDiagnoses.rowToDiagnoses(colNames, colCodes, MockDiagnosis))

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
      MainDiagnosis("HasCancer5", "C67", makeTS(2011, 12,  1)),
      LinkedDiagnosis("HasCancer5", "C67", makeTS(2011, 12, 1)),
      AssociatedDiagnosis("HasCancer5", "C67", makeTS(2011, 12, 1))
    ).toDF

    // When
    val output = McoDiagnoses.extract(config, fakeMcoData)

    // Then
    assertDFs(expected.toDF, output.toDF)
  }
}


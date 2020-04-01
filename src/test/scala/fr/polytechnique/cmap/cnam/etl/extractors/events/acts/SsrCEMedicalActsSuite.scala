// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.acts

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Event, MedicalAct, SsrCEAct}
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.sources.ssrce.SsrCeSource
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class SsrCEMedicalActsSuite extends SharedContext {

  val colNames = new SsrCeSource {}.ColNames

  val schema = StructType(
    StructField(colNames.PatientID, StringType) ::
      StructField(colNames.CamCode, StringType) ::
      StructField(colNames.StartDate, DateType) :: Nil
  )

  "isInStudy" should "return true when a study code is found in the row" in {

    // Given
    val codes = SimpleExtractorCodes(List("AAAA", "BBBB"))
    val inputArray = Array[Any]("Patient_A", "AAAA", makeTS(2010, 1, 1))
    val inputRow = new GenericRowWithSchema(inputArray, schema)

    // When
    val result = SsrCeActExtractor(codes).isInStudy(inputRow)

    // Then
    assert(result)
  }

  it should "return false when no code is found in the row" in {

    // Given
    val codes = SimpleExtractorCodes(List("AAAA", "BBBB"))
    val inputArray = Array[Any]("Patient_A", "CCCC", makeTS(2010, 1, 1))
    val inputRow = new GenericRowWithSchema(inputArray, schema)

    // When
    val result = SsrCeActExtractor(codes).isInStudy(inputRow)

    // Then
    assert(!result)
  }

  "extract" should "return a Dataset of Ssr CE Medical Acts" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val codes = SimpleExtractorCodes(List("AAAA", "CCCC"))

    val input = Seq(
      ("Patient_A", "AAAA", makeTS(2010, 1, 1)),
      ("Patient_A", "BBBB", makeTS(2010, 2, 1)),
      ("Patient_B", "CCCC", makeTS(2010, 3, 1)),
      ("Patient_B", "CCCC", makeTS(2010, 4, 1)),
      ("Patient_C", "BBBB", makeTS(2010, 5, 1))
    ).toDF(
      colNames.PatientID, colNames.CamCode, colNames.StartDate
    )

    val sources = Sources(ssrCe = Some(input))

    val expected = Seq[Event[MedicalAct]](
      SsrCEAct("Patient_A", "NA", "AAAA", 0.0, makeTS(2010, 1, 1)),
      SsrCEAct("Patient_B", "NA", "CCCC", 0.0, makeTS(2010, 3, 1)),
      SsrCEAct("Patient_B", "NA", "CCCC", 0.0, makeTS(2010, 4, 1))
    ).toDS

    // When
    val result = SsrCeActExtractor(codes).extract(sources)

    // Then
    assertDSs(result, expected)
  }

}

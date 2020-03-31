// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, EventBuilder, EventCategory}
import fr.polytechnique.cmap.cnam.etl.extractors.dcir.DcirRowExtractor


trait MockEvent extends AnyEvent with EventBuilder

object MockEventobject extends MockEvent {
  override val category: EventCategory[AnyEvent] = "NA"
}

class PrescriptionExtractorSuite extends SharedContext {

  object MockPrescriptionExtractor extends DcirRowExtractor

  "extractGroupId" should "return the group ID for done values" in {
    // Given
    val schema = StructType(
      Seq(
        StructField("FLX_DIS_DTD", StringType),
        StructField("FLX_TRT_DTD", StringType),
        StructField("FLX_EMT_TYP", StringType),
        StructField("FLX_EMT_NUM", StringType),
        StructField("FLX_EMT_ORD", StringType),
        StructField("ORG_CLE_NUM", StringType),
        StructField("DCT_ORD_NUM", StringType)
      )
    )

    val values = Array[Any]("2014-08-01", "2014-07-17", "1", "17", "0", "01C673000", "1749")
    val r = new GenericRowWithSchema(values, schema)
    val expected = "MjAxNC0wOC0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMwMDBfMTc0OQ=="

    // When
    val result = MockPrescriptionExtractor.extractGroupId(r)

    // Then
    assert(result == expected)
  }
}

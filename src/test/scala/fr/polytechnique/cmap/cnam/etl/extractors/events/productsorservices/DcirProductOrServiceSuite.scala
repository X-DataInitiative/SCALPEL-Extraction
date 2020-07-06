// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.productsorservices

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions._
import org.apache.spark.sql.DataFrame

class DcirProductOrServiceSuite extends SharedContext {
  override val debug: Boolean = true

  "extract" should "extract lpp products from raw data of the dcir (er_tip_f table)" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val dcir: DataFrame = sqlCtx.read.load("src/test/resources/test-input/DCIR_all.parquet")
    val input = Sources(dcir = Some(dcir))

    val expected = Seq[Event[ProductOrService]](
      ProductOrService("Patient_01", "liberal", "5600", 1.0, makeTS(2006, 1, 15)),
      ProductOrService("Patient_02", "liberal", "5589", 1.0, makeTS(2006, 1, 5))
    ).toDS

    // When
    val result = DcirProductOrServiceExtractor(SimpleExtractorCodes.empty).extract(input)

    // Then
    assertDSs(result, expected)
  }
}


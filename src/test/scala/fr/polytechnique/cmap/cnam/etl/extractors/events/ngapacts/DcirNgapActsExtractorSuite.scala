// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.ngapacts

import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{DcirNgapAct, Event, NgapAct}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class DcirNgapActsExtractorSuite extends SharedContext {

  "extract" should "extract ngap acts events from raw data with a ngapClass based on key letter B2 and coefficient" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val dcir: DataFrame = sqlCtx.read.load("src/test/resources/test-input/DCIR.parquet")
    val irNat: DataFrame = sqlCtx.read.load("src/test/resources/value_tables/IR_NAT_V.parquet")

    val source = new Sources(dcir = Some(dcir), irNat = Some(irNat))

    val expected = Seq[Event[NgapAct]](
      DcirNgapAct("Patient_01", "unknown_source", "1111_C_0.42", 0.0, makeTS(2006, 2, 1)),
      DcirNgapAct("Patient_01", "liberal", "1111_C_0.42", 0.0, makeTS(2006, 1, 15)),
      DcirNgapAct("Patient_01", "liberal", "1111_C_0.42", 0.0, makeTS(2006, 1, 30))
    ).toDS

    val ngapConf = NgapActConfig(
      actsCategories = List(
        new NgapWithNatClassConfig(
          ngapKeyLetters = Seq("D"),
          ngapCoefficients = Seq("0.45"),
          ngapPrsNatRefs = Seq("1111")
        )
      )
    )

    // When
    val result = DcirNgapActExtractor(ngapConf).extract(source)

    // Then
    assertDSs(result, expected)
  }


  "extract from prsNatRef" should "extract ngap acts events from raw data with a ngapClass based on prsNatRef" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val dcir: DataFrame = sqlCtx.read.load("src/test/resources/test-input/DCIR.parquet")
    val irNat: DataFrame = sqlCtx.read.load("src/test/resources/value_tables/IR_NAT_V.parquet")

    val source = new Sources(dcir = Some(dcir), irNat = Some(irNat))

    val expected = Seq[Event[NgapAct]](
      DcirNgapAct("Patient_01", "unknown_source", "1111_C_0.42", 0.0, makeTS(2006, 2, 1)),
      DcirNgapAct("Patient_01", "liberal", "1111_C_0.42", 0.0, makeTS(2006, 1, 15)),
      DcirNgapAct("Patient_01", "liberal", "1111_C_0.42", 0.0, makeTS(2006, 1, 30))
    ).toDS

    val ngapConf = NgapActConfig(
      actsCategories = List(
        new NgapWithNatClassConfig(
          ngapKeyLetters = Seq("D"),
          ngapCoefficients = Seq("0.45"),
          ngapPrsNatRefs = Seq("1111")
        )
      )
    )
    // When
    val result = DcirNgapActExtractor(ngapConf).extract(source)

    // Then
    assertDSs(result, expected)
  }

}


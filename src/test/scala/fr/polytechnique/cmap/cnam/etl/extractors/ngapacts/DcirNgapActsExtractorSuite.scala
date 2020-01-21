package fr.polytechnique.cmap.cnam.etl.extractors.ngapacts

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class DcirNgapActsExtractorSuite extends SharedContext {

  object ngapClassKeyLetterCoefficient extends NgapActClassConfig {
    val ngapKeyLetters: Seq[String] = Seq("C")
    val ngapCoefficients: Seq[String] = Seq(
      "0.42"
    )
    override val ngapPrsNatRefs: Seq[String] = Seq()
  }

  object ngapPrsNatRef extends NgapActClassConfig {
    val ngapKeyLetters: Seq[String] = Seq("D")
    val ngapCoefficients: Seq[String] = Seq(
      "0.45"
    )
    override val ngapPrsNatRefs: Seq[String] = Seq("1111")
  }

  "extract" should "extract ngap acts events from raw data with a ngapClass based on key letter B2 and coefficient" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val dcir: DataFrame = sqlCtx.read.load("src/test/resources/test-input/DCIR.parquet")
    val irNat: DataFrame = sqlCtx.read.load("src/test/resources/value_tables/IR_NAT_V.parquet")

    val source = new Sources(dcir = Some(dcir), irNat = Some(irNat))

    val expected = Seq[Event[NgapAct]](
      NgapAct("Patient_01", "A10000001", "1111_C_0.42", makeTS(2006, 2, 1)),
      NgapAct("Patient_01", "A10000001", "1111_C_0.42", makeTS(2006, 1, 15)),
      NgapAct("Patient_01", "A10000001", "1111_C_0.42", makeTS(2006, 1, 30))
    ).toDS

    val ngapConf = NgapActConfig(
      acts_categories = List(
        ngapClassKeyLetterCoefficient
      )
    )
    // When
    val result = new DcirNgapActExtractor(ngapConf).extract(source, Set.empty)

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
      NgapAct("Patient_01", "A10000001", "1111_C_0.42", makeTS(2006, 2, 1)),
      NgapAct("Patient_01", "A10000001", "1111_C_0.42", makeTS(2006, 1, 15)),
      NgapAct("Patient_01", "A10000001", "1111_C_0.42", makeTS(2006, 1, 30))
    ).toDS

    val ngapConf = NgapActConfig(
      acts_categories = List(
        ngapPrsNatRef
      )
    )
    // When
    val result = new DcirNgapActExtractor(ngapConf).extract(source, Set.empty)

    // Then
    assertDSs(result, expected)
  }

}


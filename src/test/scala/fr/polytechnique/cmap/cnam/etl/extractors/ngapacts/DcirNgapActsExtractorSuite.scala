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

  object ngapClassConsultation extends NgapActClassConfig {
    val ngapKeyLetters: Seq[String] = Seq("C")
    val ngapCoefficients: Seq[String] = Seq(
      "0.42"
    )
  }


  "extract" should "extract ngap acts events from raw data" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val ngapCoefficients = Seq("0.42")

    val dcir: DataFrame = sqlCtx.read.load("src/test/resources/test-input/DCIR.parquet")
    val irNat: DataFrame = sqlCtx.read.load("src/test/resources/value_tables/IR_NAT_V.parquet")

    val source = new Sources(dcir = Some(dcir), irNat = Some(irNat))

    val expected = Seq[Event[NgapAct]](
      NgapAct("Patient_01", "A10000001", "C_0.42", makeTS(2006, 2, 1)),
      NgapAct("Patient_01", "A10000001", "C_0.42", makeTS(2006, 1, 15)),
      NgapAct("Patient_01", "A10000001", "C_0.42", makeTS(2006, 1, 30))
    ).toDS

    val ngapConf = NgapActConfig(
      acts_categories = List(
        ngapClassConsultation
      )
    )
    // When
    val result = new DcirNgapActExtractor(ngapConf).extract(source, Set.empty)

    // Then
    assertDSs(result, expected)
  }

//  "extractGroupId" should "return the extractGroupId (healthcare practionner ID for DcirNgapActsExtractor)" in {
//
//    // Given
//    val ngapCoefficients = Seq("0.42")
//    val schema = StructType(
//      StructField("PFS_EXE_NUM", StringType) :: Nil
//    )
//    val values = Array[Any]("A10000001")
//    val row = new GenericRowWithSchema(values, schema)
//    val expected = "A10000001"
//
//    // When
//    val result =  DcirNgapActsExtractor(ngapCoefficients).extractGroupId(row)
//
//    // Then
//    assert(result == expected)
//  }
//
//  "extractCode" should "return the ngap coefficient" in {
//
//    // Given
//    val ngapCoefficients = Seq("0.42")
//    val schema = StructType(
//      StructField("PRS_ACT_CFT", DoubleType) :: Nil
//    )
//    val values = Array[Any](0.42)
//    val row = new GenericRowWithSchema(values, schema)
//    val expected = "0.42"
//
//    // When
//    val result =  DcirNgapActsExtractor(ngapCoefficients).extractCode(row, "PRS_ACT_CFT" , ngapCoefficients)
//
//    // Then
//    assert(result.get == expected)
//  }
}


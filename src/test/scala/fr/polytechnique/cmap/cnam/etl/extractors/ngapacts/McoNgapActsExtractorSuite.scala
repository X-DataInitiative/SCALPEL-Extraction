package fr.polytechnique.cmap.cnam.etl.extractors.ngapacts

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.makeTS
import org.apache.spark.sql.DataFrame

class McoNgapActsExtractorSuite extends SharedContext {

  object ngapClassKeyLetterCoefficient extends NgapActClassConfig {
    val ngapKeyLetters: Seq[String] = Seq("ABG")
    val ngapCoefficients: Seq[String] = Seq("42.0")
  }

  object ngapKeyLetter extends NgapActClassConfig {
    val ngapKeyLetters: Seq[String] = Seq("ABC")
    val ngapCoefficients: Seq[String] = Seq.empty
  }

  "extract" should "extract ngap acts events from raw data with a ngapClass based on key letter B2 and coefficient" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val mcoCe: DataFrame = sqlCtx.read.load("src/test/resources/test-input/MCO_CE.parquet")
    val source = new Sources(mcoCe = Some(mcoCe))

    val expected = Seq[Event[NgapAct]](
      McoCeFbstcNgapAct("200410", "190000059_00022621_2014", "PmsiCe_ABG_42.0", makeTS(2014, 4, 18))
    ).toDS

    val ngapConf = NgapActConfig(
      actsCategories = List(
        ngapClassKeyLetterCoefficient
      )
    )
    // When
    val result = new McoCeFbstcNgapActExtractor(ngapConf).extract(source, Set.empty)
    // Then
    assertDSs(result, expected)
  }


  "extract from prsNatRef" should "extract ngap acts events from raw data with a ngapKeyLetter only" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val mcoCe: DataFrame = sqlCtx.read.load("src/test/resources/test-input/MCO_CE.parquet")
    val source = new Sources(mcoCe = Some(mcoCe))

    val expected = Seq[Event[NgapAct]](
      McoCeFbstcNgapAct("2004100010", "390780146_00064268_2014", "PmsiCe_ABC_1.0", makeTS(2014, 7, 18))
    ).toDS

    val ngapConf = NgapActConfig(
      actsCategories = List(
        ngapKeyLetter
      )
    )
    // When
    val result = new McoCeFbstcNgapActExtractor(ngapConf).extract(source, Set.empty)
    // Then
    assertDSs(result, expected)
  }

  "extract from prsNatRef" should "extract all ngap acts events from raw MCO_FBSTC data " in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val mcoCe: DataFrame = sqlCtx.read.load("src/test/resources/test-input/MCO_CE.parquet")
    val source = new Sources(mcoCe = Some(mcoCe))

    val expected = Seq[Event[NgapAct]](
      McoCeFbstcNgapAct("2004100010", "390780146_00064268_2014", "PmsiCe_ABC_1.0", makeTS(2014, 7, 18)),
      McoCeFbstcNgapAct("200410", "190000059_00022621_2014", "PmsiCe_ABG_42.0", makeTS(2014, 4, 18)),
      McoCeFbstcNgapAct("2004100010", "390780146_00114237_2014", "PmsiCe_ACO_0", makeTS(2014, 12, 12))
    ).toDS

    val ngapConf = NgapActConfig(
      actsCategories = List.empty
    )
    // When
    val result = new McoCeFbstcNgapActExtractor(ngapConf).extract(source, Set.empty)

    // Then
    assertDSs(result, expected)
  }

  "extract from prsNatRef" should "extract all ngap acts events from raw MCO_FCSTC data " in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val mcoCe: DataFrame = sqlCtx.read.load("src/test/resources/test-input/MCO_CE.parquet")
    val source = new Sources(mcoCe = Some(mcoCe))

    val expected = Seq[Event[NgapAct]](
      McoCeFcstcNgapAct("2004100010", "390780146_00026744_2014", "PmsiCe_A   F_126936.43", makeTS(2014, 4, 4)),
      McoCeFcstcNgapAct("2004100010", "390780146_00114237_2014", "PmsiCe_ADE_802770.97", makeTS(2014, 12, 12)),
      McoCeFcstcNgapAct("2004100010", "710780214_00000130_2014", "PmsiCe_ADC_420416.2", makeTS(2014, 4, 15))
    ).toDS

    val ngapConf = NgapActConfig(
      actsCategories = List.empty
    )
    // When
    val result = new McoCeFcstcNgapActExtractor(ngapConf).extract(source, Set.empty)

    // Then
    assertDSs(result, expected)
  }

}


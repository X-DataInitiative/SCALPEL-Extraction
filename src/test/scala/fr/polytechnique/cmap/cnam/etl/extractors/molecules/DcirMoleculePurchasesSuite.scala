// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.molecules

import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.Molecule
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions._

class DcirMoleculePurchasesSuite extends SharedContext {

  val config = MoleculePurchasesConfig(List("A10"))

  val dcirColumns = Array(
    "NUM_ENQ",
    "ER_PHA_F__PHA_PRS_IDE",
    "ER_PHA_F__PHA_PRS_C13",
    "ER_PHA_F__PHA_ACT_QSN",
    "EXE_SOI_DTD"
  )

  val irPhaColumns = Array(
    "PHA_PRS_IDE",
    "PHA_CIP_C13",
    "PHA_ATC_C03"
  )

  val dosageColumns = Array(
    "PHA_PRS_IDE",
    "MOLECULE_NAME",
    "TOTAL_MG_PER_UNIT"
  )

  val allColumns = Array(
    "CIP07",
    "patientID",
    "nBoxes",
    "eventDate",
    "CIP13",
    "category",
    "moleculeName",
    "dosage",
    "totalDose"
  )

  "getInput" should "prepare the right input for the Extractor" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val dcir: DataFrame = Seq(
      (Some("patient"), Some("3541848"), None, Some(1), Some(makeTS(2006, 1, 15))),
      (Some("patient"), None, Some("3400935418487"), Some(1), Some(makeTS(2006, 1, 15))),
      (Some("patient"), Some("3541848"), Some("3400935418487"), Some(1), Some(makeTS(2006, 1, 15)))
    ).toDF(dcirColumns: _*)

    val irPha: DataFrame = Seq(
      ("3541848", "3400935418487", "A10")
    ).toDF(irPhaColumns: _*)
    val dosages: DataFrame = Seq(
      ("3541848", "GLICLAZIDE", 900)
    ).toDF(dosageColumns: _*)
    val sources = new Sources(
      dcir = Some(dcir),
      irPha = Some(irPha),
      dosages = Some(dosages)
    )
    val expected = Seq(
      ("3541848", "patient", 1, makeTS(2006, 1, 15),
        "3400935418487", "A10", "SULFONYLUREA", 900.0, 2700.0)
    ).toDF(allColumns: _*)

    //when
    val result = new DcirMoleculePurchases(config).getInput(sources).distinct()

    //then
    assertDFs(result, expected)

  }

  "isInExtractorScope" should "filter columns which contain empty event date or invalid dosage" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val dcir: DataFrame = Seq(
      (Some("patient"), Some("3541848"), Some("3400935418487"), Some(1), Some(makeTS(2006, 1, 15))),
      (Some("patient"), Some("3541848"), None, Some(-1), Some(makeTS(2006, 1, 15))), //invalid dosage
      (Some("patient"), None, Some("3400935418487"), Some(1), None), //invalid event date
      (Some("patient"), None, None, None, None) //empty date

    ).toDF(dcirColumns: _*)

    val irPha: DataFrame = Seq(
      ("3541848", "3400935418487", "A10")
    ).toDF(irPhaColumns: _*)
    val dosages: DataFrame = Seq(
      ("3541848", "GLICLAZIDE", 900)
    ).toDF(dosageColumns: _*)
    val sources = new Sources(
      dcir = Some(dcir),
      irPha = Some(irPha),
      dosages = Some(dosages)
    )
    val expected = Seq(
      ("3541848", "patient", 1, makeTS(2006, 1, 15),
        "3400935418487", "A10", "SULFONYLUREA", 900.0, 900.0)
    ).toDF(allColumns: _*)

    //when
    val extractor = new DcirMoleculePurchases(config)
    val result = extractor.getInput(sources).filter(extractor.isInExtractorScope _).distinct()

    //then
    assertDFs(result, expected)
  }

  "isInStudy" should "filter code and drug class in terms of config" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val dcir: DataFrame = Seq(
      (Some("patient"), Some("3541848"), Some("3400935418487"), Some(1), Some(makeTS(2006, 1, 15))),
      (Some("patient"), Some("00000000"), None, Some(1), Some(makeTS(2006, 1, 15))), // invalid CIP07
      (Some("patient"), Some("1234567"), None, Some(1), Some(makeTS(2006, 1, 15))) // drug class not in config
    ).toDF(dcirColumns: _*)

    val irPha: DataFrame = Seq(
      ("3541848", "3400935418487", "A10"),
      ("1234567", "340091234567", "TOTO")
    ).toDF(irPhaColumns: _*)
    val dosages: DataFrame = Seq(
      ("3541848", "GLICLAZIDE", 900)
    ).toDF(dosageColumns: _*)
    val sources = new Sources(
      dcir = Some(dcir),
      irPha = Some(irPha),
      dosages = Some(dosages)
    )
    val expected = Seq(
      ("3541848", "patient", 1, makeTS(2006, 1, 15),
        "3400935418487", "A10", "SULFONYLUREA", 900.0, 900.0)
    ).toDF(allColumns: _*)

    //when
    val extractor = new DcirMoleculePurchases(config)
    val result = extractor.getInput(sources).filter(extractor.isInStudy _).distinct()

    //then
    assertDFs(result, expected)

  }

  "build" should "build a Molecule Event from a given Row" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val dcir: DataFrame = Seq(
      (Some("patient"), Some("3541848"), None, Some(1), Some(makeTS(2006, 1, 15))),
      (Some("patient"), None, Some("3400935418487"), Some(1), Some(makeTS(2006, 1, 15))),
      (Some("patient"), Some("3541848"), Some("3400935418487"), Some(1), Some(makeTS(2006, 1, 15)))
    ).toDF(dcirColumns: _*)

    val irPha: DataFrame = Seq(
      ("3541848", "3400935418487", "A10")
    ).toDF(irPhaColumns: _*)
    val dosages: DataFrame = Seq(
      ("3541848", "GLICLAZIDE", 900)
    ).toDF(dosageColumns: _*)
    val sources = new Sources(
      dcir = Some(dcir),
      irPha = Some(irPha),
      dosages = Some(dosages)
    )
    val expected = Seq(Molecule("patient", "SULFONYLUREA", 2700.0, makeTS(2006, 1, 15))).toDS()

    //when
    val extractor = new DcirMoleculePurchases(config)
    val result = extractor.getInput(sources).flatMap(extractor.builder).distinct()

    //then
    assertDSs(result, expected)
  }


  "extract" should "extract valid data from input" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val dcir: DataFrame = Seq(
      (Some("patient"), Some("3541848"), None, Some(1), Some(makeTS(2006, 1, 15))),
      (Some("patient"), None, Some("3400935418487"), Some(1), Some(makeTS(2006, 1, 15))),
      (Some("patient"), Some("3541848"), Some("3400935418487"), Some(1), Some(makeTS(2006, 1, 15))),
      (Some("patient"), None, None, None, None),
      (Some("patient"), Some("3541848"), None, Some(-1), Some(makeTS(2006, 1, 15))),
      (Some("patient"), None, Some("3400935418487"), Some(1), None),
      (Some("patient"), Some("00000000"), None, Some(1), Some(makeTS(2006, 1, 15)))
    ).toDF(dcirColumns: _*)

    val irPha: DataFrame = sqlContext
      .read
      .parquet("src/test/resources/test-input/IR_PHA_R_With_molecules.parquet")
    val dosages: DataFrame = sqlContext
      .read
      .option("header", "true")
      .csv("src/test/resources/test-input/DOSE_PER_MOLECULE.CSV")

    val sources = new Sources(
      dcir = Some(dcir),
      irPha = Some(irPha),
      dosages = Some(dosages)
    )
    val expected = Seq(Molecule("patient", "SULFONYLUREA", 2700.0, makeTS(2006, 1, 15))).toDS()

    // When
    val result = new DcirMoleculePurchases(config).extract(sources)
    // Then
    assertDSs(result, expected)
  }

}
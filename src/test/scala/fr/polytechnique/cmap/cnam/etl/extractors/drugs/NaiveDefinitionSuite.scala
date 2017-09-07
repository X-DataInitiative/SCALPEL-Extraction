package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event}
import fr.polytechnique.cmap.cnam.study.fall._
import fr.polytechnique.cmap.cnam.study.fall.codes.{Antidepresseurs, Antihypertenseurs, Hypnotiques, Neuroleptiques}
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class NaiveDefinitionSuite extends SharedContext {

  "extract" should "work correctly based on the DrugConfig" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val inputDF = Seq(
      ("patient1", Some("9111111111111"), Some(makeTS(2014, 5, 1))),
      ("patient2", Some("3400935183644"), Some(makeTS(2014, 6, 1))),
      ("patient3", Some("3400935418487"), Some(makeTS(2014, 7, 1))),
      ("patient4", Some("3400935183644"), Some(makeTS(2014, 8, 1))),
      ("patient5", Some("3400936889651"), None),
      ("patient6", None, Some(makeTS(2014, 9, 1))),
      ("patient8", Some("3400936889651"), Some(makeTS(2014, 9, 1)))
    ).toDF("NUM_ENQ", "ER_PHA_F__PHA_PRS_C13", "EXE_SOI_DTD")

    val expected: Dataset[Event[Drug]] = Seq(
      Drug("patient2", "Antidepresseurs", 0.0, makeTS(2014, 6, 1)),
      Drug("patient4", "Antidepresseurs", 0.0, makeTS(2014, 8, 1)),
      Drug("patient8", "Antidepresseurs", 0.0, makeTS(2014, 9, 1))
    ).toDS

    val drugConfig: DrugConfig = Antidepresseurs

    // When
    val result = NaiveDefinition(inputDF, drugConfig).extract

    // Then
    assertDSs(result, expected)
  }

  it should "return expected output when DrugConfig is Neuroleptiques" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val inputDF = Seq(
      ("patient1", Some("9111111111111"), Some(makeTS(2014, 5, 1))),
      ("patient2", Some("3400930023648"), Some(makeTS(2014, 6, 1))),
      ("patient3", Some("3400935183644"), Some(makeTS(2014, 7, 1)))
    ).toDF("NUM_ENQ", "ER_PHA_F__PHA_PRS_C13", "EXE_SOI_DTD")

    val expected: Dataset[Event[Drug]] = Seq(
      Drug("patient2", "Neuroleptiques", 0.0, makeTS(2014, 6, 1))
    ).toDS

    val drugConfig: DrugConfig = Neuroleptiques

    // When
    val result = NaiveDefinition(inputDF, drugConfig).extract

    // Then
    assertDSs(result, expected)
  }

  it should "return expected output when DrugConfig is Hypnotiques" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val inputDF = Seq(
      ("patient1", Some("3400930081143"), Some(makeTS(2014, 6, 1))),
      ("patient2", Some("3400936099777"), Some(makeTS(2014, 7, 1)))
    ).toDF("NUM_ENQ", "ER_PHA_F__PHA_PRS_C13", "EXE_SOI_DTD")

    val expected: Dataset[Event[Drug]] = Seq(
      Drug("patient1", "Hypnotiques", 0.0, makeTS(2014, 6, 1)),
      Drug("patient2", "Hypnotiques", 0.0, makeTS(2014, 7, 1))
    ).toDS

    val drugConfig: DrugConfig = Hypnotiques

    // When
    val result = NaiveDefinition(inputDF, drugConfig).extract

    // Then
    assertDSs(result, expected)
  }

  it should "return expected output when DrugConfig is Antihypertenseurs" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val inputDF = Seq(
      ("patient1", Some("3400937354004"), Some(makeTS(2014, 6, 1))),
      ("patient2", Some("3400936099777"), Some(makeTS(2014, 7, 1)))
    ).toDF("NUM_ENQ", "ER_PHA_F__PHA_PRS_C13", "EXE_SOI_DTD")

    val expected: Dataset[Event[Drug]] = Seq(
      Drug("patient1", "Antihypertenseurs", 0.0, makeTS(2014, 6, 1))
    ).toDS

    val drugConfig: DrugConfig = Antihypertenseurs

    // When
    val result = NaiveDefinition(inputDF, drugConfig).extract

    // Then
    assertDSs(result, expected)
  }
}

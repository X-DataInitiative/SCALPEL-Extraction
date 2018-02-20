package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.study.fall.codes.{Antidepresseurs, Antihypertenseurs, Hypnotiques, Neuroleptiques}
import fr.polytechnique.cmap.cnam.util.functions.makeTS
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.lit

class DrugsExtractorSuite extends SharedContext{

  "formatSource" should "extract the right columns from source" in {
    //Given
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

    val source = new Sources(irPha = Some(Seq(
      (Some("9111111111111"), "toto"),
      (Some("3400935183644"), "toto"),
      (Some("3400935418487"), "toto"),
      (Some("3400936889651"), "toto")
    ).toDF("PHA_CIP_C13", "PHA_ATC_C07")
      .withColumn("PHA_NOM_PA", lit(""))
    ), dcir = Some(inputDF))

    val expected: Dataset[Purchase] = Seq(
      Purchase("patient1", "9111111111111", "toto", makeTS(2014, 5, 1)),
      Purchase("patient2", "3400935183644", "toto", makeTS(2014, 6, 1)),
      Purchase("patient3", "3400935418487", "toto", makeTS(2014, 7, 1)),
      Purchase("patient4", "3400935183644", "toto", makeTS(2014, 8, 1)),
      Purchase("patient8", "3400936889651", "toto", makeTS(2014, 9, 1))
    ).toDS()

    //When
    val result = DrugsExtractor.formatSource(source)

    //Then
    assertDSs(result, expected)
  }

  "getCorrectDrugCodes" should "extract the right columns from source" in {

    //Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val input: Dataset[Purchase] = Seq(
      Purchase("patient1", "9111111111111", "toto", makeTS(2014, 5, 1)),
      Purchase("patient2", "3400935183644", "toto", makeTS(2014, 6, 1)),
      Purchase("patient3", "3400935418487", "toto", makeTS(2014, 7, 1)),
      Purchase("patient4", "3400935183644", "toto", makeTS(2014, 8, 1)),
      Purchase("patient8", "3400936889651", "toto", makeTS(2014, 9, 1))
    ).toDS()
    val drugConfig: DrugConfig = Antidepresseurs
    val expected: Dataset[Event[Drug]] = Seq(
      Drug("patient2", "Antidepresseurs", 0.0, makeTS(2014, 6, 1)),
      Drug("patient4", "Antidepresseurs", 0.0, makeTS(2014, 8, 1)),
      Drug("patient8", "Antidepresseurs", 0.0, makeTS(2014, 9, 1))
    ).toDS
    //When
    val result = DrugsExtractor.getCorrectDrugCodes(DrugClassificationLevel.Therapeutic, input, List(drugConfig))

    //Then
    assertDSs(result, expected)
  }

  "extract" should "work correctly based on the DrugConfig Antidepresseurs" in {

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

    val source = new Sources(irPha = Some(Seq(
      (Some("9111111111111"), "toto"),
      (Some("3400935183644"), "toto"),
      (Some("3400935418487"), "toto"),
      (Some("3400936889651"), "toto")
    ).toDF("PHA_CIP_C13", "PHA_ATC_C07")
      .withColumn("PHA_NOM_PA", lit(""))
    ), dcir = Some(inputDF))

    val drugConfig: DrugConfig = Antidepresseurs

    // When
    val result = DrugsExtractor.extract(DrugClassificationLevel.Therapeutic, source, List(drugConfig))

    // Then
    assertDSs(result, expected)
  }

  "extract" should "return expected drug purchases with Therapeutic level of classification with class Neuroleptiques" in {

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

    val source = new Sources(irPha = Some(Seq(
      (Some("9111111111111"), "toto"),
      (Some("3400935183644"), "toto"),
      (Some("3400930023648"), "toto")
    ).toDF("PHA_CIP_C13", "PHA_ATC_C07")
      .withColumn("PHA_NOM_PA", lit(""))
    ), dcir = Some(inputDF))

    val drugConfig: DrugConfig = Neuroleptiques

    // When
    val result = DrugsExtractor.extract(DrugClassificationLevel.Therapeutic, source, List(drugConfig))

    // Then
    assertDSs(result, expected)
  }

  "extract" should "return expected drug purchases with Therapeutic level of classification with class Hypnotiques" in {

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

    val source = new Sources(irPha = Some(Seq(
      (Some("3400930081143"), "toto"),
      (Some("3400936099777"), "toto")
    ).toDF("PHA_CIP_C13", "PHA_ATC_C07")
      .withColumn("PHA_NOM_PA", lit(""))
    ), dcir = Some(inputDF))

    val drugConfig: DrugConfig = Hypnotiques

    // When
    val result = DrugsExtractor.extract(DrugClassificationLevel.Therapeutic, source, List(drugConfig))

    // Then
    assertDSs(result, expected)
  }

  "extract" should "return expected drug purchases with Therapeutic level of classification with class Antihypertenseurs" in {

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


    val source = new Sources(irPha = Some(Seq(
      (Some("3400937354004"), "toto"),
      (Some("3400936099777"), "toto")
    ).toDF("PHA_CIP_C13", "PHA_ATC_C07")
      .withColumn("PHA_NOM_PA", lit(""))
    ), dcir = Some(inputDF))

    val drugConfig: DrugConfig = Antihypertenseurs

    // When
    val result = DrugsExtractor.extract(DrugClassificationLevel.Therapeutic, source, List(drugConfig))

    // Then
    assertDSs(result, expected)
  }

  "extract" should "return expected drug purchases with Therapeutic level" in {

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
      ("patient8", Some("3400936889651"), Some(makeTS(2014, 9, 1))),
      ("patient1", Some("9111111111111"), Some(makeTS(2014, 5, 1))),
      ("patient2", Some("3400930023648"), Some(makeTS(2014, 6, 1)))
    ).toDF("NUM_ENQ", "ER_PHA_F__PHA_PRS_C13", "EXE_SOI_DTD")

    val expected: Dataset[Event[Drug]] = Seq(
      Drug("patient2", "Antidepresseurs", 0.0, makeTS(2014, 6, 1)),
      Drug("patient4", "Antidepresseurs", 0.0, makeTS(2014, 8, 1)),
      Drug("patient8", "Antidepresseurs", 0.0, makeTS(2014, 9, 1)),
      Drug("patient2", "Neuroleptiques", 0.0, makeTS(2014, 6, 1))
    ).toDS

    val source = new Sources(irPha = Some(Seq(
      (Some("9111111111111"), "toto"),
      (Some("3400935183644"), "N06AA04"),
      (Some("3400935418487"), "A10BB09"),
      (Some("3400936889651"), "N06AB03"),
      (Some("3400930023648"), "N05AX12")
    ).toDF("PHA_CIP_C13", "PHA_ATC_C07")
      .withColumn("PHA_NOM_PA", lit(""))
    ), dcir = Some(inputDF))

    val drugConfigAntidepresseurs: DrugConfig = Antidepresseurs
    val drugConfigNeuroleptiques: DrugConfig = Neuroleptiques
    // When
    val result = DrugsExtractor.extract(DrugClassificationLevel.Therapeutic, source, List(drugConfigAntidepresseurs, drugConfigNeuroleptiques))

    // Then
    assertDSs(result, expected)
  }

  "extract" should "return expected drug purchases with Pharmacological level" in {

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
      ("patient8", Some("3400936889651"), Some(makeTS(2014, 9, 1))),
      ("patient1", Some("9111111111111"), Some(makeTS(2014, 5, 1))),
      ("patient2", Some("3400930023648"), Some(makeTS(2014, 6, 1)))
    ).toDF("NUM_ENQ", "ER_PHA_F__PHA_PRS_C13", "EXE_SOI_DTD")

    val expected: Dataset[Event[Drug]] = Seq(
      Drug("patient2", "Antidepresseurs_Tricycliques", 0.0, makeTS(2014, 6, 1)),
      Drug("patient4", "Antidepresseurs_Tricycliques", 0.0, makeTS(2014, 8, 1)),
      Drug("patient8", "Antidepresseurs_ISRS", 0.0, makeTS(2014, 9, 1)),
      Drug("patient2", "Neuroleptiques_Autre_neuroleptique", 0.0, makeTS(2014, 6, 1))
    ).toDS

    val source = new Sources(irPha = Some(Seq(
      (Some("9111111111111"), "toto"),
      (Some("3400935183644"), "N06AA04"),
      (Some("3400935418487"), "A10BB09"),
      (Some("3400936889651"), "N06AB03"),
      (Some("3400930023648"), "N05AX12")
    ).toDF("PHA_CIP_C13", "PHA_ATC_C07")
        .withColumn("PHA_NOM_PA", lit(""))
    ), dcir = Some(inputDF))

    val drugConfigAntidepresseurs: DrugConfig = Antidepresseurs
    val drugConfigNeuroleptiques: DrugConfig = Neuroleptiques
    // When
    val result = DrugsExtractor.extract(DrugClassificationLevel.Pharmacological, source, List(drugConfigAntidepresseurs, drugConfigNeuroleptiques))

    // Then
    assertDSs(result, expected)
  }

  "extract" should "return expected drug purchases with Molecule level" in {

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
      ("patient8", Some("3400936889651"), Some(makeTS(2014, 9, 1))),
      ("patient1", Some("9111111111111"), Some(makeTS(2014, 5, 1))),
      ("patient2", Some("3400930023648"), Some(makeTS(2014, 6, 1)))
    ).toDF("NUM_ENQ", "ER_PHA_F__PHA_PRS_C13", "EXE_SOI_DTD")

    val expected: Dataset[Event[Drug]] = Seq(
      Drug("patient2", "", 0.0, makeTS(2014, 6, 1)),
      Drug("patient4", "", 0.0, makeTS(2014, 8, 1)),
      Drug("patient8", "DEXTROPROPOXYPHENE", 0.0, makeTS(2014, 9, 1)),
      Drug("patient8", "PARACETAMOL", 0.0, makeTS(2014, 9, 1)),
      Drug("patient8", "CAFEINE", 0.0, makeTS(2014, 9, 1)),
      Drug("patient2", "INSULINE LISPRO (PROTAMINE)", 0.0, makeTS(2014, 6, 1))
    ).toDS

    val source = new Sources(irPha = Some(Seq(
      (Some("9111111111111"), "toto", ""),
      (Some("3400935183644"), "N06AA04", ""),
      (Some("3400935418487"), "A10BB09", ""),
      (Some("3400936889651"), "N06AB03", "DEXTROPROPOXYPHENE + PARACETAMOL + CAFEINE"),
      (Some("3400930023648"), "N05AX12", "INSULINE LISPRO (PROTAMINE)")
    ).toDF("PHA_CIP_C13", "PHA_ATC_C07", "PHA_NOM_PA")
    ), dcir = Some(inputDF))

    val drugConfigAntidepresseurs: DrugConfig = Antidepresseurs
    val drugConfigNeuroleptiques: DrugConfig = Neuroleptiques
    // When
    val result = DrugsExtractor.extract(DrugClassificationLevel.Molecule, source, List(drugConfigAntidepresseurs, drugConfigNeuroleptiques))

    // Then
    assertDSs(result, expected)
  }


}

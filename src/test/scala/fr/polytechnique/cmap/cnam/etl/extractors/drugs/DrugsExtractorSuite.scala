package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.lit
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.study.fall.codes.{Antidepresseurs, Antihypertenseurs, Hypnotiques, Neuroleptiques}
import fr.polytechnique.cmap.cnam.study.fall.config.FallConfig.DrugsConfig
import fr.polytechnique.cmap.cnam.util.functions.makeTS

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
      (Some("9111111111111"), "toto", "GC"),
      (Some("3400935183644"), "toto", "NGC"),
      (Some("3400935418487"), "toto", "GC"),
      (Some("3400936889651"), "toto", "GC")
    ).toDF("PHA_CIP_C13", "PHA_ATC_C07","PHA_CND_TOP")
      .withColumn("molecule_combination", lit(""))
    ), dcir = Some(inputDF))

    val expected: Dataset[Purchase] = Seq(
      Purchase("patient1", "9111111111111", "toto", makeTS(2014, 5, 1), "", 1),
      Purchase("patient2", "3400935183644", "toto", makeTS(2014, 6, 1), "", 2),
      Purchase("patient3", "3400935418487", "toto", makeTS(2014, 7, 1), "", 1),
      Purchase("patient4", "3400935183644", "toto", makeTS(2014, 8, 1), "", 2),
      Purchase("patient8", "3400936889651", "toto", makeTS(2014, 9, 1), "", 1)
    ).toDS()

    val drugConf = DrugsConfig(TherapeuticLevel, List())
    //When
    val result = new DrugsExtractor(drugConf).formatSource(source)

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
      Drug("patient2", "Antidepresseurs", 1, makeTS(2014, 6, 1)),
      Drug("patient4", "Antidepresseurs", 1, makeTS(2014, 8, 1)),
      Drug("patient8", "Antidepresseurs", 1, makeTS(2014, 9, 1))
    ).toDS

    val source = new Sources(irPha = Some(Seq(
      (Some("9111111111111"), "toto", "GC"),
      (Some("3400935183644"), "toto", "GC"),
      (Some("3400935418487"), "toto", "GC"),
      (Some("3400936889651"), "toto", "GC")
    ).toDF("PHA_CIP_C13", "PHA_ATC_C07", "PHA_CND_TOP")
      .withColumn("molecule_combination", lit(""))
    ), dcir = Some(inputDF))

    val drugConf = DrugsConfig(TherapeuticLevel, List(Antidepresseurs))

    // When
    val result = new DrugsExtractor(drugConf).extract(source)

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
      Drug("patient2", "Neuroleptiques", 2, makeTS(2014, 6, 1))
    ).toDS

    val source = new Sources(irPha = Some(Seq(
      (Some("9111111111111"), "toto", "NGC"),
      (Some("3400935183644"), "toto", "NGC"),
      (Some("3400930023648"), "toto", "NGC")
    ).toDF("PHA_CIP_C13", "PHA_ATC_C07", "PHA_CND_TOP")
      .withColumn("molecule_combination", lit(""))
    ), dcir = Some(inputDF))

    val drugConf = DrugsConfig(TherapeuticLevel, List(Neuroleptiques))
    // When
    val result = new DrugsExtractor(drugConf).extract(source)

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
      Drug("patient1", "Hypnotiques", 2, makeTS(2014, 6, 1)),
      Drug("patient2", "Hypnotiques", 1, makeTS(2014, 7, 1))
    ).toDS

    val source = new Sources(irPha = Some(Seq(
      (Some("3400930081143"), "toto", "NGC"),
      (Some("3400936099777"), "toto", "GC")
    ).toDF("PHA_CIP_C13", "PHA_ATC_C07", "PHA_CND_TOP")
      .withColumn("molecule_combination", lit(""))
    ), dcir = Some(inputDF))

    val drugConf = DrugsConfig(TherapeuticLevel, List(Hypnotiques))
    // When
    val result = new DrugsExtractor(drugConf).extract(source)

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
      Drug("patient1", "Antihypertenseurs", 1, makeTS(2014, 6, 1))
    ).toDS


    val source = new Sources(irPha = Some(Seq(
      (Some("3400937354004"), "toto", "GC"),
      (Some("3400936099777"), "toto", "GC")
    ).toDF("PHA_CIP_C13", "PHA_ATC_C07", "PHA_CND_TOP")
      .withColumn("molecule_combination", lit(""))
    ), dcir = Some(inputDF))

    val drugConf = DrugsConfig(TherapeuticLevel, List(Antihypertenseurs))

    // When
    val result = new DrugsExtractor(drugConf).extract(source)

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
      Drug("patient2", "Antidepresseurs", 1, makeTS(2014, 6, 1)),
      Drug("patient4", "Antidepresseurs", 1, makeTS(2014, 8, 1)),
      Drug("patient8", "Antidepresseurs", 1, makeTS(2014, 9, 1)),
      Drug("patient2", "Neuroleptiques", 1, makeTS(2014, 6, 1))
    ).toDS

    val source = new Sources(irPha = Some(Seq(
      (Some("9111111111111"), "toto", "GC"),
      (Some("3400935183644"), "N06AA04", "GC"),
      (Some("3400935418487"), "A10BB09", "GC"),
      (Some("3400936889651"), "N06AB03", "GC"),
      (Some("3400930023648"), "N05AX12", "GC")
    ).toDF("PHA_CIP_C13", "PHA_ATC_C07", "PHA_CND_TOP")
      .withColumn("molecule_combination", lit(""))
    ), dcir = Some(inputDF))

    val drugConfigAntidepresseurs: DrugConfig = Antidepresseurs
    val drugConfigNeuroleptiques: DrugConfig = Neuroleptiques
    val drugConf = DrugsConfig(TherapeuticLevel, List(drugConfigAntidepresseurs, drugConfigNeuroleptiques))
    // When
    val result = new DrugsExtractor(drugConf).extract(source)

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
      Drug("patient2", "Antidepresseurs_Tricycliques", 1, makeTS(2014, 6, 1)),
      Drug("patient4", "Antidepresseurs_Tricycliques", 1, makeTS(2014, 8, 1)),
      Drug("patient8", "Antidepresseurs_ISRS", 1, makeTS(2014, 9, 1)),
      Drug("patient2", "Neuroleptiques_Autres_neuroleptiques", 1, makeTS(2014, 6, 1))
    ).toDS

    val source = new Sources(irPha = Some(Seq(
      (Some("9111111111111"), "toto", "GC"),
      (Some("3400935183644"), "N06AA04", "GC"),
      (Some("3400935418487"), "A10BB09", "GC"),
      (Some("3400936889651"), "N06AB03", "GC"),
      (Some("3400930023648"), "N05AX12", "GC")
    ).toDF("PHA_CIP_C13", "PHA_ATC_C07", "PHA_CND_TOP")
        .withColumn("molecule_combination", lit(""))
    ), dcir = Some(inputDF))

    val drugConfigAntidepresseurs: DrugConfig = Antidepresseurs
    val drugConfigNeuroleptiques: DrugConfig = Neuroleptiques
    val drugConf = DrugsConfig(PharmacologicalLevel, List(drugConfigAntidepresseurs, drugConfigNeuroleptiques))
    // When
    val result = new DrugsExtractor(drugConf).extract(source)

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
      Drug("patient2", "", 1, makeTS(2014, 6, 1)),
      Drug("patient4", "", 1, makeTS(2014, 8, 1)),
      Drug("patient8", "DEXTROPROPOXYPHENE", 1, makeTS(2014, 9, 1)),
      Drug("patient8", "PARACETAMOL", 1, makeTS(2014, 9, 1)),
      Drug("patient8", "CAFEINE", 1, makeTS(2014, 9, 1)),
      Drug("patient2", "INSULINE LISPRO (PROTAMINE)", 2, makeTS(2014, 6, 1))
    ).toDS

    val source = new Sources(irPha = Some(Seq(
      (Some("9111111111111"), "toto", "", "GC"),
      (Some("3400935183644"), "N06AA04", "", "GC"),
      (Some("3400935418487"), "A10BB09", "", "GC"),
      (Some("3400936889651"), "N06AB03", "DEXTROPROPOXYPHENE_PARACETAMOL_CAFEINE", "GC"),
      (Some("3400930023648"), "N05AX12", "INSULINE LISPRO (PROTAMINE)", "NGC")
    ).toDF("PHA_CIP_C13", "PHA_ATC_C07", "molecule_combination", "PHA_CND_TOP")
    ), dcir = Some(inputDF))

    val drugConfigAntidepresseurs: DrugConfig = Antidepresseurs
    val drugConfigNeuroleptiques: DrugConfig = Neuroleptiques
    val drugConf = DrugsConfig(MoleculeLevel, List(drugConfigAntidepresseurs, drugConfigNeuroleptiques))
    // When
    val result = new DrugsExtractor(drugConf).extract(source)

    // Then
    assertDSs(result, expected)
  }

  "extract" should "return expected drug purchases with MoleculeCombination level" in {

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
      Drug("patient2", "", 1, makeTS(2014, 6, 1)),
      Drug("patient4", "", 1, makeTS(2014, 8, 1)),
      Drug("patient8", "DEXTROPROPOXYPHENE_PARACETAMOL_CAFEINE", 1, makeTS(2014, 9, 1)),
      Drug("patient2", "INSULINE LISPRO (PROTAMINE)", 1, makeTS(2014, 6, 1))
    ).toDS

    val source = new Sources(irPha = Some(Seq(
      (Some("9111111111111"), "toto", "", "GC"),
      (Some("3400935183644"), "N06AA04", "", "GC"),
      (Some("3400935418487"), "A10BB09", "", "GC"),
      (Some("3400936889651"), "N06AB03", "DEXTROPROPOXYPHENE_PARACETAMOL_CAFEINE", "GC"),
      (Some("3400930023648"), "N05AX12", "INSULINE LISPRO (PROTAMINE)", "GC")
    ).toDF("PHA_CIP_C13", "PHA_ATC_C07", "molecule_combination", "PHA_CND_TOP")
    ), dcir = Some(inputDF))

    val drugConfigAntidepresseurs: DrugConfig = Antidepresseurs
    val drugConfigNeuroleptiques: DrugConfig = Neuroleptiques
    val drugConf = DrugsConfig(MoleculeCombinationLevel, List(drugConfigAntidepresseurs, drugConfigNeuroleptiques))
    // When
    val result = new DrugsExtractor(drugConf).extract(source)

    // Then
    assertDSs(result, expected)
  }

}

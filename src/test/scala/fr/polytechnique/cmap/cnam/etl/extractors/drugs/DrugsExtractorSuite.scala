// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event}
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification._
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.families.{Antidepresseurs, Antihypertenseurs, Hypnotiques, Neuroleptiques}
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.level.{Cip13Level, MoleculeCombinationLevel, PharmacologicalLevel, TherapeuticLevel}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class DrugsExtractorSuite extends SharedContext {

  "extract" should "return all drugs when empty family list is passed" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val inputDF = Seq(
      ("patient1", Some("9111111111111"), Some(
        makeTS(
          2014,
          5,
          1
        )
      ), "2014-09-01", "2014-07-17", "1", "17", "0", "01C673100", "1749"),
      ("patient2", Some("3400935183644"), Some(
        makeTS(
          2014,
          6,
          1
        )
      ), "2014-08-01", "2014-07-17", "1", "17", "0", "01C673200", "1759"),
      ("patient3", Some("3400935418487"), Some(
        makeTS(
          2014,
          7,
          1
        )
      ), "2014-08-01", "2014-07-17", "1", "17", "0", "01C673400", "1949"),
      ("patient4", Some("3400935183644"), Some(
        makeTS(
          2014,
          8,
          1
        )
      ), "2014-08-01", "2014-07-17", "1", "17", "0", "01C673500", "1733"),
      ("patient8", Some("3400936889651"), Some(
        makeTS(
          2014,
          9,
          1
        )
      ), "2014-08-01", "2014-07-17", "1", "17", "0", "01C673700", "1199")
    ).toDF(
      "NUM_ENQ",
      "ER_PHA_F__PHA_PRS_C13",
      "EXE_SOI_DTD",
      "FLX_DIS_DTD",
      "FLX_TRT_DTD",
      "FLX_EMT_TYP",
      "FLX_EMT_NUM",
      "FLX_EMT_ORD",
      "ORG_CLE_NUM",
      "DCT_ORD_NUM"
    )

    val expected: Dataset[Event[Drug]] = Seq(
      Drug(
        "patient1",
        "9111111111111",
        1,
        "MjAxNC0wOS0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMxMDBfMTc0OQ==",
        makeTS(2014, 5, 1)
      ),
      Drug(
        "patient2",
        "3400935183644",
        1,
        "MjAxNC0wOC0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMyMDBfMTc1OQ==",
        makeTS(2014, 6, 1)
      ),
      Drug(
        "patient3",
        "3400935418487",
        1,
        "MjAxNC0wOC0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzM0MDBfMTk0OQ==",
        makeTS(2014, 7, 1)
      ),
      Drug(
        "patient4",
        "3400935183644",
        1,
        "MjAxNC0wOC0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzM1MDBfMTczMw==",
        makeTS(2014, 8, 1)
      ),
      Drug(
        "patient8",
        "3400936889651",
        1,
        "MjAxNC0wOC0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzM3MDBfMTE5OQ==",
        makeTS(2014, 9, 1)
      )
    ).toDS

    val source = new Sources(
      irPha = Some(
        Seq(
          (Some("9111111111111"), "toto", "GC"),
          (Some("3400935183644"), "toto", "GC"),
          (Some("3400935418487"), "toto", "GC"),
          (Some("3400936889651"), "toto", "GC")
        ).toDF("PHA_CIP_C13", "PHA_ATC_C07", "PHA_CND_TOP")
          .withColumn("molecule_combination", lit(""))
      ), dcir = Some(inputDF)
    )

    val drugConf = DrugConfig(Cip13Level, List.empty)
    // When
    val result: Dataset[Event[Drug]] = new DrugExtractor(drugConf).extract(source, Set.empty)

    // Then
    assertDSs(result, expected)
  }

  "extract" should "work correctly based on the DrugConfig Antidepresseurs" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val inputDF = Seq(
      ("patient1", Some("9111111111111"), Some(
        makeTS(
          2014,
          5,
          1
        )
      ), "2014-09-01", "2014-07-17", "1", "17", "0", "01C673100", "1749"),
      ("patient2", Some("3400935183644"), Some(
        makeTS(
          2014,
          6,
          1
        )
      ), "2014-08-01", "2014-07-17", "1", "17", "0", "01C673200", "1759"),
      ("patient3", Some("3400935418487"), Some(
        makeTS(
          2014,
          7,
          1
        )
      ), "2014-08-01", "2014-07-17", "1", "17", "0", "01C673400", "1949"),
      ("patient4", Some("3400935183644"), Some(
        makeTS(
          2014,
          8,
          1
        )
      ), "2014-08-01", "2014-07-17", "1", "17", "0", "01C673500", "1733"),
      ("patient5", Some("3400936889651"), None, "2014-08-01", "2014-07-17", "1", "17", "0", "01C673700", "1199"),
      ("patient6", None, Some(makeTS(2014, 9, 1)), "2014-08-01", "2014-07-11", "1", "17", "0", "01C673700", "1399"),
      ("patient8", Some("3400936889651"), Some(
        makeTS(
          2014,
          9,
          1
        )
      ), "2014-08-01", "2014-07-12", "1", "17", "0", "01C673700", "1699")
    ).toDF(
      "NUM_ENQ",
      "ER_PHA_F__PHA_PRS_C13",
      "EXE_SOI_DTD",
      "FLX_DIS_DTD",
      "FLX_TRT_DTD",
      "FLX_EMT_TYP",
      "FLX_EMT_NUM",
      "FLX_EMT_ORD",
      "ORG_CLE_NUM",
      "DCT_ORD_NUM"
    )

    val expected: Dataset[Event[Drug]] = Seq(
      Drug(
        "patient2",
        "Antidepresseurs",
        1,
        "MjAxNC0wOC0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMyMDBfMTc1OQ==",
        makeTS(2014, 6, 1)
      ),
      Drug(
        "patient4",
        "Antidepresseurs",
        1,
        "MjAxNC0wOC0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzM1MDBfMTczMw==",
        makeTS(2014, 8, 1)
      ),
      Drug(
        "patient8",
        "Antidepresseurs",
        1,
        "MjAxNC0wOC0wMV8yMDE0LTA3LTEyXzFfMTdfMF8wMUM2NzM3MDBfMTY5OQ==",
        makeTS(2014, 9, 1)
      )
    ).toDS

    val source = new Sources(
      irPha = Some(
        Seq(
          (Some("9111111111111"), "toto", "GC"),
          (Some("3400935183644"), "toto", "GC"),
          (Some("3400935418487"), "toto", "GC"),
          (Some("3400936889651"), "toto", "GC")
        ).toDF("PHA_CIP_C13", "PHA_ATC_C07", "PHA_CND_TOP")
          .withColumn("molecule_combination", lit(""))
      ), dcir = Some(inputDF)
    )

    val drugConf = DrugConfig(TherapeuticLevel, List(Antidepresseurs))

    // When
    val result = new DrugExtractor(drugConf).extract(source, Set.empty)

    // Then
    assertDSs(result, expected)
  }

  "extract" should "return expected drug purchases with Therapeutic level of classification with class Neuroleptiques" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val inputDF = Seq(
      ("patient1", Some("9111111111111"), Some(
        makeTS(
          2014,
          5,
          1
        )
      ), "2014-09-01", "2014-07-17", "1", "17", "0", "01C673100", "1749"),
      ("patient2", Some("3400930023648"), Some(
        makeTS(
          2014,
          6,
          1
        )
      ), "2014-08-01", "2014-07-17", "1", "17", "0", "01C673200", "1759"),
      ("patient3", Some("3400935183644"), Some(
        makeTS(
          2014,
          7,
          1
        )
      ), "2014-08-01", "2014-07-17", "1", "17", "0", "01C673400", "1949")
    ).toDF(
      "NUM_ENQ",
      "ER_PHA_F__PHA_PRS_C13",
      "EXE_SOI_DTD",
      "FLX_DIS_DTD",
      "FLX_TRT_DTD",
      "FLX_EMT_TYP",
      "FLX_EMT_NUM",
      "FLX_EMT_ORD",
      "ORG_CLE_NUM",
      "DCT_ORD_NUM"
    )

    val expected: Dataset[Event[Drug]] = Seq(
      Drug(
        "patient2",
        "Neuroleptiques",
        2,
        "MjAxNC0wOC0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMyMDBfMTc1OQ==",
        makeTS(2014, 6, 1)
      )
    ).toDS

    val source = new Sources(
      irPha = Some(
        Seq(
          (Some("9111111111111"), "toto", "NGC"),
          (Some("3400935183644"), "toto", "NGC"),
          (Some("3400930023648"), "toto", "NGC")
        ).toDF("PHA_CIP_C13", "PHA_ATC_C07", "PHA_CND_TOP")
          .withColumn("molecule_combination", lit(""))
      ), dcir = Some(inputDF)
    )

    val drugConf = DrugConfig(TherapeuticLevel, List(Neuroleptiques))
    // When
    val result = new DrugExtractor(drugConf).extract(source, Set.empty)

    // Then
    assertDSs(result, expected)
  }

  "extract" should "return expected drug purchases with Therapeutic level of classification with class Hypnotiques" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val inputDF = Seq(
      ("patient1", Some("3400930081143"), Some(
        makeTS(
          2014,
          6,
          1
        )
      ), "2014-09-01", "2014-07-17", "1", "17", "0", "01C673100", "1749"),
      ("patient2", Some("3400936099777"), Some(
        makeTS(
          2014,
          7,
          1
        )
      ), "2014-08-01", "2014-07-17", "1", "17", "0", "01C673200", "1759")
    ).toDF(
      "NUM_ENQ",
      "ER_PHA_F__PHA_PRS_C13",
      "EXE_SOI_DTD",
      "FLX_DIS_DTD",
      "FLX_TRT_DTD",
      "FLX_EMT_TYP",
      "FLX_EMT_NUM",
      "FLX_EMT_ORD",
      "ORG_CLE_NUM",
      "DCT_ORD_NUM"
    )

    val expected: Dataset[Event[Drug]] = Seq(
      Drug(
        "patient1",
        "Hypnotiques",
        2,
        "MjAxNC0wOS0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMxMDBfMTc0OQ==",
        makeTS(2014, 6, 1)
      ),
      Drug(
        "patient2",
        "Hypnotiques",
        1,
        "MjAxNC0wOC0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMyMDBfMTc1OQ==",
        makeTS(2014, 7, 1)
      )
    ).toDS

    val source = new Sources(
      irPha = Some(
        Seq(
          (Some("3400930081143"), "toto", "NGC"),
          (Some("3400936099777"), "toto", "GC")
        ).toDF("PHA_CIP_C13", "PHA_ATC_C07", "PHA_CND_TOP")
          .withColumn("molecule_combination", lit(""))
      ), dcir = Some(inputDF)
    )

    val drugConf = DrugConfig(TherapeuticLevel, List(Hypnotiques))
    // When
    val result = new DrugExtractor(drugConf).extract(source, Set.empty)

    // Then
    assertDSs(result, expected)
  }

  "extract" should "return expected drug purchases with Therapeutic level of classification with class Antihypertenseurs" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val inputDF = Seq(
      ("patient1", Some("3400937354004"), Some(
        makeTS(
          2014,
          6,
          1
        )
      ), "2014-09-01", "2014-07-17", "1", "17", "0", "01C673100", "1749"),
      ("patient2", Some("3400936099777"), Some(
        makeTS(
          2014,
          7,
          1
        )
      ), "2014-08-01", "2014-07-17", "1", "17", "0", "01C673200", "1759")
    ).toDF(
      "NUM_ENQ",
      "ER_PHA_F__PHA_PRS_C13",
      "EXE_SOI_DTD",
      "FLX_DIS_DTD",
      "FLX_TRT_DTD",
      "FLX_EMT_TYP",
      "FLX_EMT_NUM",
      "FLX_EMT_ORD",
      "ORG_CLE_NUM",
      "DCT_ORD_NUM"
    )

    val expected: Dataset[Event[Drug]] = Seq(
      Drug(
        "patient1",
        "Antihypertenseurs",
        1,
        "MjAxNC0wOS0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMxMDBfMTc0OQ==",
        makeTS(2014, 6, 1)
      )
    ).toDS


    val source = new Sources(
      irPha = Some(
        Seq(
          (Some("3400937354004"), "toto", "GC"),
          (Some("3400936099777"), "toto", "GC")
        ).toDF("PHA_CIP_C13", "PHA_ATC_C07", "PHA_CND_TOP")
          .withColumn("molecule_combination", lit(""))
      ), dcir = Some(inputDF)
    )

    val drugConf = DrugConfig(TherapeuticLevel, List(Antihypertenseurs))

    // When
    val result = new DrugExtractor(drugConf).extract(source, Set.empty)

    // Then
    assertDSs(result, expected)
  }

  "extract" should "return expected drug purchases with Therapeutic level" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val inputDF = Seq(
      ("patient1", Some("9111111111111"), Some(
        makeTS(
          2014,
          5,
          1
        )
      ), "2014-09-01", "2014-07-17", "1", "17", "0", "01C673100", "1749"),
      ("patient2", Some("3400935183644"), Some(
        makeTS(
          2014,
          6,
          1
        )
      ), "2014-08-01", "2014-07-17", "1", "17", "0", "01C673200", "1759"),
      ("patient3", Some("3400935418487"), Some(
        makeTS(
          2014,
          7,
          1
        )
      ), "2014-08-01", "2014-07-17", "1", "17", "0", "01C673400", "1949"),
      ("patient4", Some("3400935183644"), Some(
        makeTS(
          2014,
          8,
          1
        )
      ), "2014-08-01", "2014-07-17", "1", "17", "0", "01C673500", "1733"),
      ("patient5", Some("3400936889651"), None, "2014-08-01", "2014-07-17", "1", "17", "0", "01C673700", "1199"),
      ("patient6", None, Some(makeTS(2014, 9, 1)), "2014-09-01", "2014-07-17", "1", "17", "0", "01C673100", "1959"),
      ("patient8", Some("3400936889651"), Some(
        makeTS(
          2014,
          9,
          1
        )
      ), "2014-09-01", "2014-07-17", "1", "17", "0", "01C673100", "2749"),
      ("patient1", Some("9111111111111"), Some(
        makeTS(
          2014,
          5,
          1
        )
      ), "2014-09-01", "2014-07-17", "1", "17", "0", "01C673100", "1749"),
      ("patient2", Some("3400930023648"), Some(
        makeTS(
          2014,
          6,
          1
        )
      ), "2014-08-01", "2014-07-17", "1", "17", "0", "01C673200", "1759")
    ).toDF(
      "NUM_ENQ",
      "ER_PHA_F__PHA_PRS_C13",
      "EXE_SOI_DTD",
      "FLX_DIS_DTD",
      "FLX_TRT_DTD",
      "FLX_EMT_TYP",
      "FLX_EMT_NUM",
      "FLX_EMT_ORD",
      "ORG_CLE_NUM",
      "DCT_ORD_NUM"
    )

    val expected: Dataset[Event[Drug]] = Seq(
      Drug(
        "patient2",
        "Antidepresseurs",
        1,
        "MjAxNC0wOC0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMyMDBfMTc1OQ==",
        makeTS(2014, 6, 1)
      ),
      Drug(
        "patient4",
        "Antidepresseurs",
        1,
        "MjAxNC0wOC0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzM1MDBfMTczMw==",
        makeTS(2014, 8, 1)
      ),
      Drug(
        "patient8",
        "Antidepresseurs",
        1,
        "MjAxNC0wOS0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMxMDBfMjc0OQ==",
        makeTS(2014, 9, 1)
      ),
      Drug(
        "patient2",
        "Neuroleptiques",
        1,
        "MjAxNC0wOC0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMyMDBfMTc1OQ==",
        makeTS(2014, 6, 1)
      )
    ).toDS

    val source = new Sources(
      irPha = Some(
        Seq(
          (Some("9111111111111"), "toto", "GC"),
          (Some("3400935183644"), "N06AA04", "GC"),
          (Some("3400935418487"), "A10BB09", "GC"),
          (Some("3400936889651"), "N06AB03", "GC"),
          (Some("3400930023648"), "N05AX12", "GC")
        ).toDF("PHA_CIP_C13", "PHA_ATC_C07", "PHA_CND_TOP")
          .withColumn("molecule_combination", lit(""))
      ), dcir = Some(inputDF)
    )

    val drugConfigAntidepresseurs: DrugClassConfig = Antidepresseurs
    val drugConfigNeuroleptiques: DrugClassConfig = Neuroleptiques
    val drugConf = DrugConfig(TherapeuticLevel, List(drugConfigAntidepresseurs, drugConfigNeuroleptiques))
    // When
    val result = new DrugExtractor(drugConf).extract(source, Set.empty)
    // Then
    assertDSs(result, expected)
  }

  "extract" should "return expected drug purchases with Pharmacological level" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val inputDF = Seq(
      ("patient1", Some("9111111111111"), Some(
        makeTS(
          2014,
          5,
          1
        )
      ), "2014-09-01", "2014-07-17", "1", "17", "0", "01C673100", "1749"),
      ("patient2", Some("3400935183644"), Some(
        makeTS(
          2014,
          6,
          1
        )
      ), "2014-08-01", "2014-07-17", "1", "17", "0", "01C673200", "1759"),
      ("patient3", Some("3400935418487"), Some(
        makeTS(
          2014,
          7,
          1
        )
      ), "2014-08-01", "2014-07-17", "1", "17", "0", "01C673400", "1949"),
      ("patient4", Some("3400935183644"), Some(
        makeTS(
          2014,
          8,
          1
        )
      ), "2014-08-01", "2014-07-17", "1", "17", "0", "01C673500", "1733"),
      ("patient5", Some("3400936889651"), None, "2014-08-01", "2014-07-17", "1", "17", "0", "01C673700", "1199"),
      ("patient6", None, Some(makeTS(2014, 9, 1)), "2014-09-01", "2014-07-17", "1", "17", "0", "01C673100", "1959"),
      ("patient8", Some("3400936889651"), Some(
        makeTS(
          2014,
          9,
          1
        )
      ), "2014-09-01", "2014-07-17", "1", "17", "0", "01C673100", "2749"),
      ("patient1", Some("9111111111111"), Some(
        makeTS(
          2014,
          5,
          1
        )
      ), "2014-09-01", "2014-07-17", "1", "17", "0", "01C673100", "1749"),
      ("patient2", Some("3400930023648"), Some(
        makeTS(
          2014,
          6,
          1
        )
      ), "2014-08-01", "2014-07-17", "1", "17", "0", "01C673200", "1759")
    ).toDF(
      "NUM_ENQ",
      "ER_PHA_F__PHA_PRS_C13",
      "EXE_SOI_DTD",
      "FLX_DIS_DTD",
      "FLX_TRT_DTD",
      "FLX_EMT_TYP",
      "FLX_EMT_NUM",
      "FLX_EMT_ORD",
      "ORG_CLE_NUM",
      "DCT_ORD_NUM"
    )

    val expected: Dataset[Event[Drug]] = Seq(
      Drug(
        "patient2",
        "Antidepresseurs_Tricycliques",
        1,
        "MjAxNC0wOC0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMyMDBfMTc1OQ==",
        makeTS(2014, 6, 1)
      ),
      Drug(
        "patient4",
        "Antidepresseurs_Tricycliques",
        1,
        "MjAxNC0wOC0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzM1MDBfMTczMw==",
        makeTS(2014, 8, 1)
      ),
      Drug(
        "patient8",
        "Antidepresseurs_ISRS",
        1,
        "MjAxNC0wOS0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMxMDBfMjc0OQ==",
        makeTS(2014, 9, 1)
      ),
      Drug(
        "patient2",
        "Neuroleptiques_Autres_neuroleptiques",
        1,
        "MjAxNC0wOC0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMyMDBfMTc1OQ==",
        makeTS(2014, 6, 1)
      )
    ).toDS

    val source = new Sources(
      irPha = Some(
        Seq(
          (Some("9111111111111"), "toto", "GC"),
          (Some("3400935183644"), "N06AA04", "GC"),
          (Some("3400935418487"), "A10BB09", "GC"),
          (Some("3400936889651"), "N06AB03", "GC"),
          (Some("3400930023648"), "N05AX12", "GC")
        ).toDF("PHA_CIP_C13", "PHA_ATC_C07", "PHA_CND_TOP")
          .withColumn("molecule_combination", lit(""))
      ), dcir = Some(inputDF)
    )

    val drugConfigAntidepresseurs: DrugClassConfig = Antidepresseurs
    val drugConfigNeuroleptiques: DrugClassConfig = Neuroleptiques
    val drugConf = DrugConfig(PharmacologicalLevel, List(drugConfigAntidepresseurs, drugConfigNeuroleptiques))
    // When
    val result = new DrugExtractor(drugConf).extract(source, Set.empty)

    // Then
    assertDSs(result, expected)
  }

  "extract" should "return expected drug purchases with MoleculeCombination level" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val inputDF = Seq(
      ("patient1", Some("9111111111111"), Some(
        makeTS(
          2014,
          5,
          1
        )
      ), "2014-09-01", "2014-07-17", "1", "17", "0", "01C673100", "1749"),
      ("patient2", Some("3400935183644"), Some(
        makeTS(
          2014,
          6,
          1
        )
      ), "2014-08-01", "2014-07-17", "1", "17", "0", "01C673200", "1759"),
      ("patient3", Some("3400935418487"), Some(
        makeTS(
          2014,
          7,
          1
        )
      ), "2014-08-01", "2014-07-17", "1", "17", "0", "01C673400", "1949"),
      ("patient4", Some("3400935183644"), Some(
        makeTS(
          2014,
          8,
          1
        )
      ), "2014-08-01", "2014-07-17", "1", "17", "0", "01C673500", "1733"),
      ("patient5", Some("3400936889651"), None, "2014-08-01", "2014-07-17", "1", "17", "0", "01C673700", "1199"),
      ("patient6", None, Some(makeTS(2014, 9, 1)), "2014-09-01", "2014-07-17", "1", "17", "0", "01C673100", "1959"),
      ("patient8", Some("3400936889651"), Some(
        makeTS(
          2014,
          9,
          1
        )
      ), "2014-09-01", "2014-07-17", "1", "17", "0", "01C673100", "2749"),
      ("patient1", Some("9111111111111"), Some(
        makeTS(
          2014,
          5,
          1
        )
      ), "2014-09-01", "2014-07-17", "1", "17", "0", "01C673100", "1749"),
      ("patient2", Some("3400930023648"), Some(
        makeTS(
          2014,
          6,
          1
        )
      ), "2014-08-01", "2014-07-17", "1", "17", "0", "01C673200", "1759")
    ).toDF(
      "NUM_ENQ",
      "ER_PHA_F__PHA_PRS_C13",
      "EXE_SOI_DTD",
      "FLX_DIS_DTD",
      "FLX_TRT_DTD",
      "FLX_EMT_TYP",
      "FLX_EMT_NUM",
      "FLX_EMT_ORD",
      "ORG_CLE_NUM",
      "DCT_ORD_NUM"
    )

    val expected: Dataset[Event[Drug]] = Seq(
      Drug(
        "patient2",
        "N06AA04",
        1,
        "MjAxNC0wOC0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMyMDBfMTc1OQ==",
        makeTS(2014, 6, 1)
      ),
      Drug(
        "patient4",
        "N06AA04",
        1,
        "MjAxNC0wOC0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzM1MDBfMTczMw==",
        makeTS(2014, 8, 1)
      ),
      Drug(
        "patient8",
        "DEXTROPROPOXYPHENE_PARACETAMOL_CAFEINE",
        1,
        "MjAxNC0wOS0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMxMDBfMjc0OQ==",
        makeTS(2014, 9, 1)
      ),
      Drug(
        "patient2",
        "INSULINE LISPRO (PROTAMINE)",
        1,
        "MjAxNC0wOC0wMV8yMDE0LTA3LTE3XzFfMTdfMF8wMUM2NzMyMDBfMTc1OQ==",
        makeTS(2014, 6, 1)
      )
    ).toDS

    val irPha = Seq(
      (Some("9111111111111"), "toto", "toto", "GC"),
      (Some("3400935183644"), "N06AA04", "N06AA04", "GC"),
      (Some("3400935418487"), "A10BB09", "A10BB09", "GC"),
      (Some("3400936889651"), "N06AB03", "DEXTROPROPOXYPHENE_PARACETAMOL_CAFEINE", "GC"),
      (Some("3400930023648"), "N05AX12", "INSULINE LISPRO (PROTAMINE)", "GC")
    ).toDF("PHA_CIP_C13", "PHA_ATC_C07", "molecule_combination", "PHA_CND_TOP")

    val source = new Sources(irPha = Some(irPha), dcir = Some(inputDF))

    val drugConfigAntidepresseurs: DrugClassConfig = Antidepresseurs
    val drugConfigNeuroleptiques: DrugClassConfig = Neuroleptiques
    val drugConf = DrugConfig(MoleculeCombinationLevel, List(drugConfigAntidepresseurs, drugConfigNeuroleptiques))
    // When
    val result = new DrugExtractor(drugConf).extract(source, Set("SHIT"))

    // Then
    assertDSs(result, expected)
  }

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

    val values = Array[Any]("2014-08-01", "2014-08-17", "1", "17", "0", "01C673000", "1759")
    val r = new GenericRowWithSchema(values, schema)
    val expected = "MjAxNC0wOC0wMV8yMDE0LTA4LTE3XzFfMTdfMF8wMUM2NzMwMDBfMTc1OQ=="

    val drugConf = DrugConfig(Cip13Level, List.empty)

    // When
    val result = new DrugExtractor(drugConf).extractGroupId(r)

    // Then
    assert(result == expected)
  }


}

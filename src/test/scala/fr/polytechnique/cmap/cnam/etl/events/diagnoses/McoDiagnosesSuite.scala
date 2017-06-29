package fr.polytechnique.cmap.cnam.etl.events.diagnoses

import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class McoDiagnosesSuite extends SharedContext {

  // todo: update test
  "extract" should "extract diagnosis events from raw data" in {

    /*val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val codes = List("C67")
    val input: DataFrame = Seq(
      ("HasCancer1", Some("C669"), Some("C672"), Some("C643"), Some(12), Some(2011), 11,
        Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12))),
      ("HasCancer1", Some("C679"), Some("C691"), Some("C643"),Some(12), Some(2011), 11,
        Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12))),
      ("HasCancer2", Some("C669"), Some("C672"), Some("C643"), Some(12), Some(2011), 11,
        None, Some(makeTS(2011, 12, 12))),
      ("HasCancer3", Some("C669"), Some("C672"), Some("C643"), Some(12), Some(2011), 11,
        None, None),
      ("HasCancer4", Some("C669"), Some("C672"), Some("C643"), None, None, 11,
        None, Some(makeTS(2011, 12, 12))),
      ("HasCancer5", Some("C679"), Some("B672"), Some("C673"), Some(1), Some(2010), 31,
        Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12))),
      ("MustBeDropped1", None, None, None, Some(1), Some(2010), 31,
        Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12))),
      ("MustBeDropped2", None, Some("7"), None, Some(1), Some(2010), 31,
        Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12)))
    ).toDF("NUM_ENQ", "MCO_D.ASS_DGN", "MCO_B.DGN_PAL", "MCO_B.DGN_REL", "MCO_B.SOR_MOI",
      "MCO_B.SOR_ANN", "MCO_B.SEJ_NBJ", "ENT_DAT", "SOR_DAT")

    val expected: DataFrame = Seq(
      MainDiagnosis("HasCancer1", "C67", makeTS(2011, 12,  1)),
      AssociatedDiagnosis("HasCancer1", "C67", makeTS(2011, 12,  1)),
      MainDiagnosis("HasCancer2", "C67", makeTS(2011, 12,  1)),
      MainDiagnosis("HasCancer3", "C67", makeTS(2011, 11, 20)),
      MainDiagnosis("HasCancer4", "C67", makeTS(2011, 12,  1)),
      LinkedDiagnosis("HasCancer5", "C67", makeTS(2011, 12, 1)),
      AssociatedDiagnosis("HasCancer5", "C67", makeTS(2011, 12, 1))
    ).toDF

    // When
    val output = McoDiagnoses.extract(input, codes, codes, codes)

    // Then
    assertDFs(expected.toDF, output.toDF)
    */

  }
}


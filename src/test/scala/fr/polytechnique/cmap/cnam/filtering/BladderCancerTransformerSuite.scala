package fr.polytechnique.cmap.cnam.filtering

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames
import fr.polytechnique.cmap.cnam.utilities.functions._



class BladderCancerTransformerSuite extends SharedContext{

  "extractNarrowCancer" should "capture the event when code found in Diagnostic principal" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val input = Seq(
      ("JohnDoe", Some("C67*"), Some("TOTO"), Some("TOTO")),
      ("I m fine", null, null, Some("TOTO"))
    ).toDF("patientID", "DP", "DR", "DAS")

    // When
    import BladderCancerTransformer._
    val result = input.extractNarrowCancer


    // Then
    assert(result.count()==1)
  }

  "extractNarrowCancer" should "capture the event when code found in Diagnostic relié" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val input = Seq(
      ("JohnDoe", Some("TOTO"), Some("C67*"), Some("TOTO")),
      ("I m fine", null, null, Some("TOTO"))
    ).toDF("patientID", "DP", "DR", "DAS")

    // When
    import BladderCancerTransformer._
    val result = input.extractNarrowCancer


    // Then
    assert(result.count()==1)
  }

  "extractNarrowCancer" should "capture the event when code combination found with DP" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val input = Seq(
      ("C77 & C67", Some("C77*"), Some("TOTO"), Some("C67*")),
      ("C78 & C67", Some("C78*"), Some("TOTO"), Some("C67*")),
      ("C79 & C67", Some("C79*"), Some("TOTO"), Some("C67*")),
      ("I m fine", null, null, Some("TOTO"))
    ).toDF("patientID", "DP", "DR", "DAS")

    // When
    import BladderCancerTransformer._
    val result = input.extractNarrowCancer


    // Then
    assert(result.count()==3)
  }

  "extractNarrowCancer" should "capture the event when code combination found with DR" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val input = Seq(
      ("C67 & C77", Some("TOTO"), Some("C77*"), Some("C67*")),
      ("C67 & C78", Some("TOTO"), Some("C78*"), Some("C67*")),
      ("C67 & C79", Some("TOTO"), Some("C79*"), Some("C67*")),
      ("I m fine", null, null, Some("TOTO"))
    ).toDF("patientID", "DP", "DR", "DAS")

    // When
    import BladderCancerTransformer._
    val result = input.extractNarrowCancer


    // Then
    assert(result.count()==3)
  }


  "extractNarrowCancer" should "not take C67 alone in DAS position" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val input = Seq(
      ("I'm a bit sick", Some("TOTO"), Some("TUTU"), Some("C67*"))
    ).toDF("patientID", "DP", "DR", "DAS")

    // When
    import BladderCancerTransformer._
    val result = input.extractNarrowCancer


    // Then
    assert(result.count()==0)
  }

  "transform" should "return the correct result" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val data = Seq(
      ("Patient1", Some("C67*"), Some("TUTU"), Some("TATA"), Some(12), Some(2011),
        11, Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12))),
      ("Patient2", Some("TOTO"), Some("C78*"), Some("C67*"), Some(12), Some(2011),
        11, null, Some(makeTS(2011, 12, 12))),
      ("Patient3", Some("TOTO"), Some("TUTU"), Some("TATA"), Some(12), Some(2011),
        11, null, null)
    ).toDF("NUM_ENQ", "MCO_B.DGN_PAL", "MCO_B.DGN_REL", "MCO_D.ASS_DGN", "MCO_B.SOR_MOI", "MCO_B.SOR_ANN",
      "MCO_B.SEJ_NBJ", "ENT_DAT", "SOR_DAT")
    val input = new Sources(pmsiMco=Some(data))
    val expected = Seq(
      Event("Patient1", "disease", "bladderCancer", 1, makeTS(2011, 12, 1), None),
      Event("Patient2", "disease", "bladderCancer", 1, makeTS(2011, 12, 1), None)
    ).toDF

    // When
    val output = BladderCancerTransformer.transform(input)

    // Then
    import RichDataFrames._
    output.show()
    expected.show()
    assert(output.toDF === expected)
  }
}

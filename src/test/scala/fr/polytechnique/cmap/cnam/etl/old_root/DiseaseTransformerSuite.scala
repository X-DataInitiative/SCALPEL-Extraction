package fr.polytechnique.cmap.cnam.etl.old_root

import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class DiseaseTransformerSuite extends fakeMcoDataFixture {

  /* TODO: improve this test. It should check that the methods are called without calling them
   * i.e. this test should not require dummy data.
   */
  "DiseaseTransformer" should "use child transformers' transform method in its own transform " +
    "method" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val mcoData: DataFrame = fakeMcoData
    val imbData: DataFrame = sqlContext.read
      .load("src/test/resources/expected/IR_IMB_R.parquet")
    val input = new Sources(irImb=Some(imbData), pmsiMco=Some(mcoData))

    val expected = Seq(
      Event("Patient_02", "disease", "C67", 1, makeTS(2006, 3, 13), None), // IMB event
      Event("HasCancer1", "disease", "C67", 1, makeTS(2011, 12, 1), None), // expected.MCO events
      Event("HasCancer2", "disease", "C67", 1, makeTS(2011, 12, 1), None),
      Event("HasCancer3", "disease", "C67", 1, makeTS(2011, 11, 20), None),
      Event("HasCancer4", "disease", "C67", 1, makeTS(2011, 12, 1), None),
      Event("HasCancer5", "disease", "C67", 1, makeTS(2011, 12, 1), None)
    ).toDF

    // When
    // TODO: use some mock DiseaseTransformers to check the method calls instead
    val output = DiseaseTransformer.transform(input)

    // Then
    // TODO: check that all the transform are called instead
    assertDFs(output.toDF, expected)
 }

}

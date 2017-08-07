package fr.polytechnique.cmap.cnam.etl.old_root

import java.sql.Timestamp
import scala.collection.JavaConversions._
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.old_root.McoDiseaseTransformer._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions._


trait fakeMcoDataFixture extends SharedContext {

  def fakeMcoData = {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    Seq(
      ("HasCancer1", Some("C669"), Some("C668"), Some("C651"), Some("C672"), Some("C643"),
        Some(12), Some(2011), 11, "01122011", "12122011"
        ),
      ("HasCancer2", Some("C669"), Some("C668"), Some("C651"), Some("C672"), Some("C643"),
        Some(12), Some(2011), 11, null, "12122011"
        ),
      ("HasCancer3", Some("C669"), Some("C668"), Some("C651"), Some("C672"), Some("C643"),
        Some(12), Some(2011), 11, null, null
        ),
      ("HasCancer4", Some("C669"), Some("C668"), Some("C651"), Some("C672"), Some("C643"),
        null, null, 11, null, "12122011"
        ),
      ("HasCancer5", Some("C679"), Some("C678"), Some("C671"), Some("B672"), Some("C673"),
        Some(1), Some(2010), 31, "01122011", "12122011"
        ),
      ("MustBeDropped1", null, null, null, null, null,
        Some(1), Some(2010), 31, "01122011", "12122011"
        ),
      ("MustBeDropped2", null, null, Some("C556"), Some("7"), null,
        Some(1), Some(2010), 31, "01122011", "12122011"
        )
    ).toDF("NUM_ENQ", "MCO_D__ASS_DGN", "MCO_UM__DGN_PAL", "MCO_UM__DGN_REL", "MCO_B__DGN_PAL",
      "MCO_B__DGN_REL", "MCO_B__SOR_MOI", "MCO_B__SOR_ANN", "MCO_B__SEJ_NBJ", "ENT_DAT",
      "SOR_DAT")
  }

}

class McoDiseaseTransformerSuite extends fakeMcoDataFixture {


  "extractMcoDisease" should "filter out lines that does not contain the right DiseaseCode" in {
    // Given
    val input = fakeMcoData

    // When
    val output = input.select(mcoInputColumns: _*).extractMcoDisease

    // Then
    assert(output.where(col("patientID") contains "MustBeDropped").count == 0)
  }

  "estimateEventDate" should "estimate a stay starting date using MCO available data" in {
    // Given
    val input = fakeMcoData
    val expected: List[Timestamp] = (List.fill(2)(makeTS(2011, 12, 1)) :+
      makeTS(2011, 11, 20)) ::: List.fill(4)(makeTS(2011, 12, 1))

    // When
   val output = input.select(mcoInputColumns: _*)
     .estimateStayStartTime
     .select("eventDate")
     .takeAsList(expected.length)
     .toList
     .map(_.getAs[Timestamp]("eventDate"))

    // Then
//    expected.foreach(println)
//    output.foreach(println)
    assert(output == expected)
  }

  it should "consider null values of MCO_B.SEJ_NBJ as 0" in {
    // Given
    val fakeMCOWithNullSEJ = {
      val sqlCtx = sqlContext
      import sqlCtx.implicits._

      Seq(
        ("HasCancer1", Some("C669"), Some("C668"), Some("C651"), Some("C672"), Some("C643"),
          Some(12), Some(2011), Some(11), "01122011", "12122011"
          ),
        ("HasCancer2", Some("C669"), Some("C668"), Some("C651"), Some("C672"), Some("C643"),
          Some(12), Some(2011), null, null, "12122011"
          ),
        ("HasCancer3", Some("C669"), Some("C668"), Some("C651"), Some("C672"), Some("C643"),
          Some(12), Some(2011), Some(11), null, null
          ),
        ("HasCancer3.1", Some("C669"), Some("C668"), Some("C651"), Some("C672"), Some("C643"),
          Some(12), Some(2011), null, null, null
          )
      ).toDF(fakeMcoData.columns: _*)
    }

    val input = fakeMCOWithNullSEJ
    val expected: List[Timestamp] = List(makeTS(2011, 12, 1), makeTS(2011, 12, 12),
      (makeTS(2011, 11, 20)), makeTS(2011, 12, 1))

    // When
    val output = input.select(mcoInputColumns: _*)
      .estimateStayStartTime
      .select("eventDate")
      .takeAsList(expected.length)
      .toList
      .map(_.getAs[Timestamp]("eventDate"))

    // Then
//    expected.foreach(println)
//    output.foreach(println)
    assert(output == expected)
  }

  "transform" should "return a pretty Dataset" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val data = fakeMcoData
    val input = new Sources(pmsiMco=Some(data))
    val expected = Seq(
      Event("HasCancer1", "disease", "C67", 1, makeTS(2011, 12, 1), None),
      Event("HasCancer2", "disease", "C67", 1, makeTS(2011, 12, 1), None),
      Event("HasCancer3", "disease", "C67", 1, makeTS(2011, 11, 20), None),
      Event("HasCancer4", "disease", "C67", 1, makeTS(2011, 12, 1), None),
      Event("HasCancer5", "disease", "C67", 1, makeTS(2011, 12, 1), None)
    ).toDF

    // When
    val output = McoDiseaseTransformer.transform(input)

    // Then
    assertDFs(output.toDF, expected)
 }

}

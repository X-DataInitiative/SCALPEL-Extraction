package fr.polytechnique.cmap.cnam.filtering

import scala.collection.JavaConversions._
import java.sql.Timestamp

import org.apache.spark.sql.functions._

import fr.polytechnique.cmap.cnam.utilities.RichDataFrames
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.filtering.McoDiseaseTransformer._
import fr.polytechnique.cmap.cnam.utilities.functions._


trait fakeMcoDataFixture extends SharedContext {

  def fakeMcoData = {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    Seq(
      ("HasCancer1", Some("C669"), Some("C668"), Some("C651"), Some("C672"), Some("C643"),
        Some(12), Some(2011), 11, Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12))
        ),
      ("HasCancer2", Some("C669"), Some("C668"), Some("C651"), Some("C672"), Some("C643"),
        Some(12), Some(2011), 11, null, Some(makeTS(2011, 12, 12))
        ),
      ("HasCancer3", Some("C669"), Some("C668"), Some("C651"), Some("C672"), Some("C643"),
        Some(12), Some(2011), 11, null, null
        ),
      ("HasCancer4", Some("C669"), Some("C668"), Some("C651"), Some("C672"), Some("C643"),
        null, null, 11, null, Some(makeTS(2011, 12, 12))
        ),
      ("HasCancer5", Some("C679"), Some("C678"), Some("C671"), Some("B672"), Some("C673"),
        Some(1), Some(2010), 31, Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12))
        ),
      ("MustBeDropped1", null, null, null, null, null,
        Some(1), Some(2010), 31, Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12))
        ),
      ("MustBeDropped2", null, null, Some("C556"), Some("7"), null,
        Some(1), Some(2010), 31, Some(makeTS(2011, 12, 1)), Some(makeTS(2011, 12, 12))
        )
    ).toDF("NUM_ENQ", "MCO_D.ASS_DGN", "MCO_UM.DGN_PAL", "MCO_UM.DGN_REL", "MCO_B.DGN_PAL",
      "MCO_B.DGN_REL", "MCO_B.SOR_MOI", "MCO_B.SOR_ANN", "MCO_B.SEJ_NBJ", "ENT_DAT",
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
    import RichDataFrames._
    assert(output.toDF === expected)
  }

}

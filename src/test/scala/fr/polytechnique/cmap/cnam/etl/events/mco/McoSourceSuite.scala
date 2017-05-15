package fr.polytechnique.cmap.cnam.etl.events.mco

import java.sql.Timestamp
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class McoSourceSuite extends SharedContext with McoSource {

  lazy val fakeMcoData = {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    Seq(
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
  }

  "estimateStayStartTime" should "estimate a stay starting date using MCO available data" in {

    // Given
    val input = fakeMcoData
    val expected: List[Timestamp] = (List.fill(2)(makeTS(2011, 12, 1)) :+ makeTS(2011, 11, 20)) :::
      List.fill(4)(makeTS(2011, 12, 1))


    // When
    val output: List[Timestamp] = input
      .estimateStayStartTime
      .select(NewColumns.EstimatedStayStart)
      .take(expected.length)
      .map(_.getAs[Timestamp](0))
      .toList

    // Then
    assert(output.map(_.getTime()).sorted == expected.map(_.getTime()).sorted)
  }
}

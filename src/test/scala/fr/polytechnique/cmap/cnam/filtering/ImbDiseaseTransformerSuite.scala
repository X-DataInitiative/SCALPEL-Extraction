package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.filtering.ImbDiseaseTransformer._
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames._
import java.sql.Timestamp

import fr.polytechnique.cmap.cnam.utilities.RichDataFrames

class ImbDiseaseTransformerSuite extends SharedContext {

  "extractImbDisease" should "filter out lines that does not contain the right DiseaseCode" in {

    // Given
    val input: DataFrame = sqlContext.read
      .load("src/test/resources/expected/IR_IMB_R.parquet")

    // When
    val result: DataFrame = input.select(ImbDiseaseTransformer.imbInputColumns:_*).extractImbDisease

    // Then
    assert(result.count == 1)
  }

  "transform" should "return a pretty Dataset" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input: DataFrame = sqlContext.read.load("src/test/resources/expected/IR_IMB_R.parquet")
    val source = new Sources(irImb = Some(input))
    val expected = Seq(
      Event(
        patientID = "Patient_02",
        category = "disease",
        eventId = "C67",
        weight = 1,
        start = Timestamp.valueOf("2006-03-13 00:00:00"),
        end = None
      )
    ).toDF

    // When
    val result = ImbDiseaseTransformer.transform(source)

    // Then
    assert(result.toDF === expected)

  }
  "transform" should "filter events with None start" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input:DataFrame = Seq(
      ("Patient_01", "CIM10", "C67",1, None,Some(Timestamp.valueOf("2011-12-1 00:00:00"))),
      ("Patient_02", "CIM10", "C67",1, None,None),
      ("id3", "CIM10", "C67",1,  Some(Timestamp.valueOf("2011-12-1 00:00:00")),Some(Timestamp.valueOf("2011-12-1 00:00:00")))
    ).toDF("patientID", "imbEncoding","disease", "BEN_NAI_MOI", "eventDate", "BEN_DCD_DTE")

    val expected:DataFrame = Seq(
      ("id3", "CIM10", "C67",1,  Some(Timestamp.valueOf("2011-12-1 00:00:00")),Some(Timestamp.valueOf("2011-12-1 00:00:00")))
    ).toDF("patientID", "imbEncoding","disease", "BEN_NAI_MOI", "eventDate", "BEN_DCD_DTE")
    // When
    val output = ImbDiseaseTransformer.imbDataFrame(input).extractImbDisease

    // Then
    assert(output === expected)
  }
}
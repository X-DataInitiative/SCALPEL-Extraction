package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.mockito.Mockito.mock
import fr.polytechnique.cmap.cnam.{SharedContext, util}

class DrugSuite extends SharedContext {

  val patientID: String = "patientID"
  val timestamp: Timestamp = mock(classOf[Timestamp])

  "apply" should "allow creation of Drug event" in {

    // Given
    val expected = Event[Drug](
      patientID, Drug.category, "NA", "Drug1", 0.1, timestamp, None
    )

    // When
    val result = Drug(patientID, "Drug1", 0.1, timestamp)

    // Then
    assert(result == expected)
  }

  "fromRow" should "create extract corresponding column values and create Drug event correctly" in {

    // Given
    val sqlCtx = sqlContext
    import util.functions.makeTS
    import sqlCtx.implicits._

    val inputDF = Seq(
      ("patientId", "drugName", 0.1, makeTS(2014, 5, 5))
    ).toDF("pId", "dname", "weigh", "eventDate")

    val expected = Drug("patientId", "drugName", 0.1, makeTS(2014, 5, 5))

    // When
    val result = Drug.fromRow(inputDF.first, "pId", "dname", "weigh")

    // Then
    assert(result == expected)
  }

}

package fr.polytechnique.cmap.cnam.etl.events

import org.scalatest.FlatSpecLike
import fr.polytechnique.cmap.cnam.util.functions._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}


class ClassificationSuite extends FlatSpecLike {

  "apply" should "return correct Event" in {
    // Given
    val patientID = "Stevie"
    val groupID = "42"
    val name = "GHMDA233"
    val date = makeTS(2016, 1, 1)
    val expected = Event("Stevie", "ghm", "42", "GHMDA233", 0.0, makeTS(2016, 1, 1), None)

    // When
    val result = GHMClassification(patientID, groupID, name, date)

    // Then
    assert(result == expected)
  }

  "fromRow" should "convert the row accordingly" in {
    // Given
    val schema = StructType(
      StructField("patientID", StringType) ::
        StructField("groupID", StringType) ::
        StructField("name", StringType) ::
        StructField("eventDate", TimestampType) :: Nil)

    val values = Array[Any]("Stevie", "42", "GHMDA233", makeTS(2016, 1, 1))

    val row = new GenericRowWithSchema(values, schema)

    val expected = Event("Stevie", "ghm", "42", "GHMDA233", 0.0, makeTS(2016, 1, 1), None)

    // When
    val result = GHMClassification.fromRow(row)

    // Then
    assert(result == expected)
  }


}

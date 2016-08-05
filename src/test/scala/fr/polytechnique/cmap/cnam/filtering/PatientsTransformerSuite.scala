package fr.polytechnique.cmap.cnam.filtering

import java.sql.Timestamp
import fr.polytechnique.cmap.cnam.SharedContext
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * @author Daniel de Paula
  */
class PatientsTransformerSuite extends SharedContext {

  "transform" should "return the correct data in a Dataset[Patient] for a known input" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val givenDf: DataFrame = sqlCtx.read.parquet("src/test/resources/expected/DCIR.parquet")
    val sources = new Sources(dcir = Some(givenDf))

    // When
    val result = PatientsTransformer.transform(sources)
    val expectedResult = Seq(
      Patient(
        patientID = "Patient_01",
        gender = 2,
        birthDate = Timestamp.valueOf("1975-01-01 00:00:00"),
        deathDate = None
      ),
      Patient(
        patientID = "Patient_02",
        gender = 1,
        birthDate = Timestamp.valueOf("1959-01-01 00:00:00"),
        deathDate = Some(Timestamp.valueOf("2009-03-13 00:00:00"))
      )
    )

    // Then
    assert(result.collect().toSeq == expectedResult)
  }
}

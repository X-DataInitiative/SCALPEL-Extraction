package fr.polytechnique.cmap.cnam.etl.extractors.prestations

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class DcirPrestationsSuite extends SharedContext {

  "extract" should "extract prestations events from raw data" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val medicalSpeCodes = List("42")
    val nonMedicalSpeCodes = List("42")
    val input = spark.read.parquet("src/test/resources/test-input/DCIR.parquet")

    val expected = Seq[Event[PrestationSpeciality]](
      MedicalPrestation("Patient_01", "A10000001", "42", makeTS(1600, 1, 1)),
      MedicalPrestation("Patient_01", "A10000001", "42", makeTS(2006, 1, 15)),
      MedicalPrestation("Patient_01", "A10000001", "42", makeTS(2006, 1, 30)),
      NonMedicalPrestation("Patient_01", "A10000001", "42", makeTS(1600, 1, 1)),
      NonMedicalPrestation("Patient_01", "A10000001", "42", makeTS(2006, 1, 15)),
      NonMedicalPrestation("Patient_01", "A10000001", "42", makeTS(2006, 1, 30)),
      NonMedicalPrestation("Patient_02", "A10000005", "42", makeTS(2006, 1, 5)),
      NonMedicalPrestation("Patient_02", "A10000005", "42", makeTS(2006, 1, 15)),
      NonMedicalPrestation("Patient_02", "A10000005", "42", makeTS(2006, 1, 30))
    ).toDS

    // When
    val result = DcirPrestations(medicalSpeCodes, nonMedicalSpeCodes).extract(input)

    // Then
    assertDSs(result, expected)
  }
}


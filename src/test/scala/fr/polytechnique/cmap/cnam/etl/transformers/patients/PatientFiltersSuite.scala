// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.patients

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.extractors.patients.PatientsConfig
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class PatientFiltersSuite extends SharedContext {

  "transform" should "return the correct data in a Dataset[Patient] for a known input" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val config = PatientsConfig(ageReferenceDate = makeTS(2006, 1, 1))
    val input: Dataset[Patient] = Seq(
      Patient("Patient_01", 1, makeTS(1945, 1, 1), None),
      Patient("Patient_02", 1, makeTS(1956, 2, 1), Some(makeTS(2009, 3, 13))),
      Patient("Patient_03", 2, makeTS(1937, 3, 1), Some(makeTS(1980, 4, 1))),
      Patient("Patient_04", 2, makeTS(1966, 2, 1), Some(makeTS(2009, 3, 13))),
      Patient("Patient_05", 1, makeTS(1935, 4, 1), Some(makeTS(2020, 3, 13))),
      Patient("Patient_06", 3, makeTS(1920, 8, 1), Some(makeTS(1980, 8, 1))),
      Patient("Patient_07", 3, makeTS(2000, 8, 1), Some(makeTS(1980, 8, 1)))
    ).toDS()

    // When
    val result = new PatientFilters(config).filterPatients(input)
    val expected: Dataset[Patient] = Seq(
      Patient("Patient_01", 1, makeTS(1945, 1, 1), None),
      Patient("Patient_02", 1, makeTS(1956, 2, 1), Some(makeTS(2009, 3, 13))),
      Patient("Patient_03", 2, makeTS(1937, 3, 1), Some(makeTS(1980, 4, 1))),
      Patient("Patient_04", 2, makeTS(1966, 2, 1), Some(makeTS(2009, 3, 13)))
    ).toDS()

    // Then
    assertDSs(result, expected)
  }

}

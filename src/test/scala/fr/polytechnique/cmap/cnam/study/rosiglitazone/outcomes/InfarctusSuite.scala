package fr.polytechnique.cmap.cnam.study.rosiglitazone.outcomes

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class InfarctusSuite extends SharedContext {
  "transform" should "return only found I21* or I22* in diagnoses" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq[Event[Diagnosis]](
      McoMainDiagnosis("PatientA", "I2100", makeTS(2010, 1, 1)),
      McoLinkedDiagnosis("PatientB", "I2120", makeTS(2010, 2, 1)),
      McoAssociatedDiagnosis("PatientC", "I2200", makeTS(2010, 3, 1)),

      McoMainDiagnosis("PatientD", "ABC", makeTS(2010, 1, 1)),
      McoLinkedDiagnosis("PatientA", "P50", makeTS(2010, 2, 1))
    ).toDS

    val expected = Seq(
      Outcome("PatientA", Infarctus.outcomeName, makeTS(2010, 1, 1)),
      Outcome("PatientB", Infarctus.outcomeName, makeTS(2010, 2, 1)),
      Outcome("PatientC", Infarctus.outcomeName, makeTS(2010, 3, 1))
    ).toDS

    // When
    val result = Infarctus.transform(input)

    // Then
    assertDSs(result, expected)
  }

  "transform" should "return an empty Seq" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq[Event[Diagnosis]](
      McoMainDiagnosis("PatientA", "CX50", makeTS(2010, 1, 1)),
      McoLinkedDiagnosis("PatientB", "YNWA", makeTS(2010, 2, 1)),
      McoAssociatedDiagnosis("PatientC", "LIV", makeTS(2010, 3, 1))
    ).toDS

    val expected = Seq.empty[Event[Outcome]]

    // When
    val result = Infarctus.transform(input)

    // Then
    assertDSs(result, expected.toDS)
  }

}

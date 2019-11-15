// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.pioglitazone.outcomes

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.util.functions._

class NaiveBladderCancerSuite extends SharedContext {

  "transform" should "return only found C67 diagnoses" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq[Event[Diagnosis]](
      McoMainDiagnosis("PatientA", "C67", makeTS(2010, 1, 1)),
      McoLinkedDiagnosis("PatientA", "C67", makeTS(2010, 2, 1)),
      McoAssociatedDiagnosis("PatientA", "C67", makeTS(2010, 3, 1)),
      ImbDiagnosis("PatientA", "C67", makeTS(2010, 4, 1)),
      McoMainDiagnosis("PatientA", "ABC", makeTS(2010, 5, 1))
    ).toDS

    val expected = Seq(
      Outcome("PatientA", NaiveBladderCancer.outcomeName, makeTS(2010, 1, 1))
    )

    // When
    val result = NaiveBladderCancer.transform(input)

    // Then
    assertDFs(result.toDF, expected.toDF)
  }
}

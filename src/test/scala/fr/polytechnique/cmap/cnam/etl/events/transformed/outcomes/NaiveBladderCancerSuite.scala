package fr.polytechnique.cmap.cnam.etl.events.outcomes

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.diagnoses._
import fr.polytechnique.cmap.cnam.etl.events.molecules.Molecule
import fr.polytechnique.cmap.cnam.study.pioglitazone.NaiveBladderCancer
import fr.polytechnique.cmap.cnam.util.functions._

class NaiveBladderCancerSuite extends SharedContext {

  "transform" should "return only found C67 diagnoses" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      MainDiagnosis("PatientA", "C67", makeTS(2010, 1, 1)),
      LinkedDiagnosis("PatientA", "C67", makeTS(2010, 2, 1)),
      AssociatedDiagnosis("PatientA", "C67", makeTS(2010, 3, 1)),
      ImbDiagnosis("PatientA", "C67", makeTS(2010, 4, 1)),
      MainDiagnosis("PatientA", "ABC", makeTS(2010, 5, 1)),
      Molecule("PatientA", "PIOGLITAZONE", 100.0, makeTS(2010, 6, 1))
    ).toDS

    val expected = Seq(
      Outcome("PatientA", NaiveBladderCancer.name, makeTS(2010, 1, 1))
    )

    // When
    val result = NaiveBladderCancer.transform(input)

    // Then
    assertDFs(result.toDF, expected.toDF)
  }
}
